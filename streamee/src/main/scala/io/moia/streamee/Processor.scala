/*
 * Copyright 2018 MOIA GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.moia.streamee

import akka.actor.CoordinatedShutdown
import akka.actor.CoordinatedShutdown.PhaseServiceRequestsDone
import akka.stream.scaladsl.{ Flow, GraphDSL, Keep, Sink, Source, Unzip, Zip }
import akka.stream.{ ActorAttributes, FlowShape, Materializer, OverflowStrategy, Supervision }
import org.apache.logging.log4j.scala.Logging
import scala.concurrent.Promise

/**
  * Runs a domain logic process (Akka Streams flow) accepting requests and producing responses. See
  * [[Processor.apply]] for details.
  */
object Processor extends Logging {

  final case class NotCorrelated[A, B](a: A, b: B)
      extends Exception(s"Request $a and response $b are not correlated!")

  /**
    * Runs a domain logic process (Akka Streams flow) accepting requests and producing responses.
    * Requests offered via the returned queue are pushed into the given `process`. Once responses
    * are available, the promise given together with the request is completed with success. If the
    * process back-pressures, offered requests are dropped (fail fast).
    *
    * A task is registered with Akka Coordinated Shutdown in the "service-requests-done" phase to
    * ensure that no more requests are accepted and all in-flight requests have been processed
    * before continuing with the shutdown.
    *
    * '''Attention''': the given domain logic process must emit exactly one response for every
    * request and the sequence of the elements in the process must be maintained! Give a specific
    * `correlated` function to verify this.
    *
    * @param process domain logic process from request to response
    * @param settings settings for processors (from application.conf or reference.conf)
    * @param shutdown Akka Coordinated Shutdown
    * @param correlated correlation between request and response, by default always true
    * @param mat materializer to run the stream
    * @tparam A request type
    * @tparam B response type
    * @return queue for request-promise pairs
    */
  def apply[A, B](
      process: Flow[A, B, Any],
      settings: ProcessorSettings,
      shutdown: CoordinatedShutdown,
      correlated: (A, B) => Boolean = (_: A, _: B) => true
  )(implicit mat: Materializer): Processor[A, B] = {
    val (sourceQueueWithComplete, done) =
      Source
        .queue[(A, Promise[B])](settings.bufferSize, OverflowStrategy.dropNew) // Must be 1: for 0 offers could "hang", for larger values completing would not work, see: https://github.com/akka/akka/issues/25349
        .via(embed(process, settings.maxNrOfInFlightRequests))
        .toMat(Sink.foreach {
          case (response, (request, promisedResponse)) if !correlated(request, response) =>
            promisedResponse.tryFailure(NotCorrelated(request, response))
          case (response, (_, promisedResponse)) =>
            promisedResponse.trySuccess(response)
        })(Keep.both)
        .withAttributes(ActorAttributes.supervisionStrategy(_ => Supervision.Resume)) // The stream must not die!
        .run()

    shutdown.addTask(PhaseServiceRequestsDone, "processor") { () =>
      sourceQueueWithComplete.complete()
      done // `sourceQueueWithComplete.watchCompletion` seems broken, hence use `done`
    }

    sourceQueueWithComplete
  }

  private def embed[A, B](process: Flow[A, B, Any], bufferSize: Int) =
    Flow.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._
      val nest =
        builder.add(Flow[(A, Promise[B])].map {
          case (request, promisedResponse) => (request, (request, promisedResponse))
        })
      val unzip = builder.add(Unzip[A, (A, Promise[B])]())
      val zip   = builder.add(Zip[B, (A, Promise[B])]())
      val buf   = builder.add(Flow[(A, Promise[B])].buffer(bufferSize, OverflowStrategy.backpressure))

      // format: OFF
      nest ~> unzip.in
              unzip.out0 ~> process ~> zip.in0
              unzip.out1 ~>   buf   ~> zip.in1
      // format: ON

      FlowShape(nest.in, zip.out)
    })
}
