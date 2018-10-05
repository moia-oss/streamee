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

import akka.Done
import akka.actor.{ CoordinatedShutdown, Scheduler }
import akka.http.scaladsl.model.StatusCodes.ServiceUnavailable
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.ExceptionHandler
import akka.stream.{ Materializer, QueueOfferResult }
import akka.stream.scaladsl.Flow
import org.apache.logging.log4j.scala.Logging
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.FiniteDuration

/**
  * Factories and more for [[Processor]]s.
  */
object Processor extends Logging {

  /**
    * Signals that a [[Processor]] cannot process requests at this time. Only relevant for
    * permanent [[Processor]]s.
    */
  final case class ProcessorUnavailable(name: String)
      extends Exception(s"Processor $name cannot accept requests at this time!")

  final case class UnexpectedQueueOfferResult(result: QueueOfferResult)
      extends Exception(s"QueueOfferResult $result was not expected!")

  /**
    * Maps [[ProcessorUnavailable]] exceptions to HTTP status code 503 "Service Unavailable".
    */
  implicit val processorUnavailableHandler: ExceptionHandler =
    ExceptionHandler {
      case ProcessorUnavailable(name) =>
        complete(ServiceUnavailable -> s"Processor $name cannot accept offers at this time!")
    }

  /**
    * Creates a per-request [[Processor]] and also registers it with coordinated shutdown.
    *
    * When the `process` method of such a per-request processor is called, it runs the process in a
    * sub-flow for the given single request. The returned `Future` is either completed successfully
    * with the response or failed if the processor does not create the response in time. Notice that
    * there is no back pressure over requests due to using sub-flows.
    *
    * @param process domain logic process from request to response
    * @param timeout maximum duration for the request to be processed; must be positive!
    * @param name name, used e.g. in [[ProcessorUnavailable]] exceptions
    * @param shutdown Akka Coordinated Shutdown
    * @tparam A request type
    * @tparam B response type
    * @return [[Processor]] for offering requests and shutting down, already registered with coordinated shutdown
    */
  def perRequest[A, B](
      process: Flow[A, B, Any],
      timeout: FiniteDuration,
      name: String,
      shutdown: CoordinatedShutdown
  )(implicit ec: ExecutionContext, mat: Materializer, scheduler: Scheduler): Processor[A, B] =
    perRequest(process, timeout, name).registerWithCoordinatedShutdown(shutdown)

  /**
    * Creates a per-request [[Processor]].
    *
    * When the `process` method of such a per-request processor is called, it runs the process in a
    * sub-flow for the given single request. The returned `Future` is either completed successfully
    * with the response or failed if the processor does not create the response in time. Notice that
    * there is no back pressure over requests due to using sub-flows.
    *
    * @param process domain logic process from request to response
    * @param timeout maximum duration for the request to be processed; must be positive!
    * @param name name, used e.g. in [[ProcessorUnavailable]] exceptions
    * @tparam A request type
    * @tparam B response type
    * @return [[Processor]] for offering requests and shutting down
    */
  def perRequest[A, B](
      process: Flow[A, B, Any],
      timeout: FiniteDuration,
      name: String
  )(implicit ec: ExecutionContext, mat: Materializer, scheduler: Scheduler): Processor[A, B] =
    new PerRequestProcessor(process, timeout, name)

  /**
    * Creates a permanent [[Processor]] and also registers it with coordinated shutdown.
    *
    * When the `process` method of such a permanent processor is called, it emits the request into
    * the given process. The returned `Future` is either completed successfully with the response or
    * failed if the process back pressures or does not create the response not in time. Notice that
    * for permanent processors correlation functions between request and response need to be given.
    *
    * @param process domain logic process from request to response
    * @param timeout maximum duration for the request to be processed; must be positive!
    * @param name name, used e.g. in [[ProcessorUnavailable]] exceptions
    * @param bufferSize size of the buffer of the input queue of the permanent processor
    * @param shutdown Akka Coordinated Shutdown
    * @param correlateRequest correlation function for the request
    * @param correlateResponse correlation function for the response
    * @tparam A request type
    * @tparam B response type
    * @return [[Processor]] for offering requests and shutting down
    */
  def permanent[A, B, C](process: Flow[A, B, Any],
                         timeout: FiniteDuration,
                         name: String,
                         bufferSize: Int,
                         shutdown: CoordinatedShutdown)(
      correlateRequest: A => C,
      correlateResponse: B => C
  )(implicit ec: ExecutionContext, mat: Materializer, scheduler: Scheduler): Processor[A, B] =
    permanent(process, timeout, name, bufferSize)(correlateRequest, correlateResponse)
      .registerWithCoordinatedShutdown(shutdown)

  /**
    * Creates a permanent [[Processor]].
    *
    * When the `process` method of such a permanent processor is called, it emits the request into
    * the given process. The returned `Future` is either completed successfully with the response or
    * failed if the process back pressures or does not create the response not in time. Notice that
    * for permanent processors correlation functions between request and response need to be given.
    *
    * @param process domain logic process from request to response
    * @param timeout maximum duration for the request to be processed; must be positive!
    * @param name name, used e.g. in [[ProcessorUnavailable]] exceptions
    * @param bufferSize size of the buffer of the input queue of the permanent processor
    * @param correlateRequest correlation function for the request
    * @param correlateResponse correlation function for the response
    * @tparam A request type
    * @tparam B response type
    * @return [[Processor]] for offering requests and shutting down
    */
  def permanent[A, B, C](process: Flow[A, B, Any],
                         timeout: FiniteDuration,
                         name: String,
                         bufferSize: Int)(
      correlateRequest: A => C,
      correlateResponse: B => C
  )(implicit ec: ExecutionContext, mat: Materializer, scheduler: Scheduler): Processor[A, B] =
    new PermanentProcessor(process, timeout, name, bufferSize, correlateRequest, correlateResponse)
}

/**
  * Processes requests: for a given request, a `Future` for the response is returned. Also allows
  * for shutdown and registration with Akka Coordinated Shutdown.
  */
trait Processor[A, B] {

  /**
    * Processes a request.
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
  final def registerWithCoordinatedShutdown(coordinatedShutdown: CoordinatedShutdown): this.type = {
    coordinatedShutdown.addTask(CoordinatedShutdown.PhaseServiceRequestsDone, "processor") { () =>
      shutdown()
    }
    this
  }
}
