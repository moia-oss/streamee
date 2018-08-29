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
  * Runs a domain logic process (Akka Streams flow) for processing commands into results. See
  * [[Processor.apply]] for details.
  */
object Processor extends Logging {

  /**
    * Runs a domain logic process (Akka Streams flow) for processing commands into results.
    * Commands offered via the returned queue are pushed into the given `process`. Once results are
    * available the promise given together with the command is completed with success. If the
    * process back-pressures, offered commands are dropped.
    *
    * A task is registered with Akka Coordinated Shutdown in the "service-requests-done" phase to
    * ensure that no more commands are accepted and all in-flight commands have been processed
    * before continuing with the shutdown.
    *
    * '''Attention''': the given domain logic process must emit exactly one result for every
    * command and the sequence of the elements in the process must be maintained!
    *
    * @param process domain logic process from command to result
    * @param settings settings for processors (from application.conf or reference.conf)
    * @param shutdown Akka Coordinated Shutdown
    * @param mat materializer to run the stream
    * @tparam C command type
    * @tparam R result type
    * @return queue for command-promise pairs
    */
  def apply[C, R](process: Flow[C, R, Any],
                  settings: ProcessorSettings,
                  shutdown: CoordinatedShutdown)(
      implicit mat: Materializer
  ): Processor[C, R] = {
    val (sourceQueueWithComplete, done) =
      Source
        .queue[(C, Promise[R])](settings.bufferSize, OverflowStrategy.dropNew) // Must be 1: for 0 offers could "hang", for larger values completing would not work, see: https://github.com/akka/akka/issues/25349
        .via(bypassPromise(process, settings.maxNrOfInFlightCommands))
        .toMat(Sink.foreach { case (r, promisedR) => promisedR.trySuccess(r) })(Keep.both)
        .withAttributes(ActorAttributes.supervisionStrategy(_ => Supervision.Resume)) // The stream must not die!
        .run()
    shutdown.addTask(PhaseServiceRequestsDone, "processor") { () =>
      sourceQueueWithComplete.complete()
      done // `sourceQueueWithComplete.watchCompletion` seems broken, hence use `done`
    }
    sourceQueueWithComplete
  }

  private def bypassPromise[C, R](process: Flow[C, R, Any], parallelsim: Int) =
    Flow.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._
      val unzip  = builder.add(Unzip[C, Promise[R]]())
      val zip    = builder.add(Zip[R, Promise[R]]())
      val buffer = builder.add(Flow[Promise[R]].buffer(parallelsim, OverflowStrategy.backpressure))

      // format: OFF
      unzip.out0 ~> process ~> zip.in0
      unzip.out1 ~>  buffer  ~> zip.in1
      // format: ON

      FlowShape(unzip.in, zip.out)
    })
}
