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

import akka.actor.{ CoordinatedShutdown, Scheduler }
import akka.http.scaladsl.model.StatusCodes.ServiceUnavailable
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.ExceptionHandler
import akka.stream.{ Materializer, OverflowStrategy, QueueOfferResult }
import akka.stream.scaladsl.{ Flow, Keep, Sink, Source }
import akka.stream.QueueOfferResult.{ Dropped, Enqueued }
import akka.Done
import io.moia.streamee.Processor.{ ProcessorUnavailable, UnexpectedQueueOfferResult }
import org.apache.logging.log4j.scala.Logging
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration.{ Duration, FiniteDuration }

/**
  * Runs a domain logic process (Akka Streams flow) accepting requests and producing responses. See
  * [[Processor.apply]] for details.
  */
object Processor extends Logging {

  final case class ProcessorUnavailable(name: String)
      extends Exception(s"Processor $name cannot accept requests at this time!")

  final case class UnexpectedQueueOfferResult(result: QueueOfferResult)
      extends Exception(s"QueueOfferResult $result was not expected!")

  implicit val processorUnavailableHandler: ExceptionHandler =
    ExceptionHandler {
      case ProcessorUnavailable(name) =>
        complete(ServiceUnavailable -> s"Processor $name cannot accept offers at this time!")
    }

  /**
    * Creates a [[Processor]] and also registers it with coordinated shutdown.
    *
    * @param process domain logic process from request to response
    * @param timeout maximum duration for the request to be processed; must be positive!
    * @param name name, used e.g. in [[ProcessorUnavailable]] exceptions
    * @param shutdown Akka Coordinated Shutdown
    * @tparam A request type
    * @tparam B response type
    * @return [[Processor]] for offering requests and shutting down, already registered with coordinated shutdown
    */
  def apply[A, B](
      process: Flow[A, B, Any],
      timeout: FiniteDuration,
      name: String,
      shutdown: CoordinatedShutdown
  )(implicit ec: ExecutionContext, mat: Materializer, scheduler: Scheduler): Processor[A, B] =
    Processor(process, timeout, name).registerWithCoordinatedShutdown(shutdown)

  /**
    * Creates a [[Processor]].
    *
    * @param process domain logic process from request to response
    * @param timeout maximum duration for the request to be processed; must be positive!
    * @param name name, used e.g. in [[ProcessorUnavailable]] exceptions
    * @tparam A request type
    * @tparam B response type
    * @return [[Processor]] for offering requests and shutting down
    */
  def apply[A, B](
      process: Flow[A, B, Any],
      timeout: FiniteDuration,
      name: String
  )(implicit ec: ExecutionContext, mat: Materializer, scheduler: Scheduler): Processor[A, B] =
    new ProcessorImpl(process, timeout, name)
}

/**
  * Process requests with a running process or shut it down.
  */
sealed trait Processor[A, B] {

  /**
    * Runs this processor's process for a single request. The returned `Future` is either completed
    * successfully with the response or failed if the process back pressures or does not create the
    * response not in time.
    *
    * @param request request to be processed
    * @return `Future` for the response
    */
  def process(request: A): Future[B]

  /**
    * Shuts down this processor. Already accepted requests are still processed, but no new ones are
    * accepted. The returened `Future` is completed once all requests have been processed.
    *
    * @return `Future` signaling that all requests have been processed
    */
  def shutdown(): Future[Done]

  /**
    * Registers shutdown of this processor during Akka Coordinated Shutdown in the
    * "service-requests-done" phase.
    *
    * @return this instance
    */
  def registerWithCoordinatedShutdown(coordinatedShutdown: CoordinatedShutdown): this.type = {
    coordinatedShutdown.addTask(CoordinatedShutdown.PhaseServiceRequestsDone, "processor") { () =>
      shutdown()
    }
    this
  }
}

private final class ProcessorImpl[A, B](
    process: Flow[A, B, Any],
    timeout: FiniteDuration,
    name: String
)(implicit ec: ExecutionContext, mat: Materializer, scheduler: Scheduler)
    extends Processor[A, B] {

  require(timeout > Duration.Zero, s"timeout must be positive, but was $timeout!")

  private val (queue, done) =
    Source
      .queue[(A, Promise[B])](1, OverflowStrategy.dropNew) // No need to use a large buffer, beause a substream is run for each request!
      .toMat(Sink.foreach {
        case (request, response) =>
          response.completeWith(Source.single(request).via(process).runWith(Sink.head))
      })(Keep.both)
      .run()

  override def process(request: A): Future[B] = {
    val response = ExpiringPromise[B](timeout, s"from processor $name for request $request")
    queue
      .offer((request, response))
      .flatMap {
        case Enqueued => response.future
        case Dropped  => Future.failed(ProcessorUnavailable(name))
        case other    => Future.failed(UnexpectedQueueOfferResult(other))
      }
  }

  override def shutdown(): Future[Done] = {
    queue.complete()
    done
  }
}
