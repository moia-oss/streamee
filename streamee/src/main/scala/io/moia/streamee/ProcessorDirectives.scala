/*
 * Copyright (c) MOIA GmbH
 */

package io.moia.streamee

import akka.actor.CoordinatedShutdown.Reason
import akka.actor.{ CoordinatedShutdown, Scheduler }
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.{ complete, onSuccess }
import akka.http.scaladsl.server.Route
import akka.stream.QueueOfferResult.{ Dropped, Enqueued }
import akka.stream.scaladsl.SourceQueue
import org.apache.logging.log4j.scala.Logging
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ ExecutionContext, Promise }

/**
  * Directives to be used with processors.
  */
object ProcessorDirectives extends Logging {

  private final object ProcessorCompletedOrFailed extends Reason

  /**
    * Offers the given command to the given processor thereby using an [[ExpiringPromise]] with the
    * given `timeout` and handles the returned `OfferQueueResult` (from Akka Streams
    * `SourceQueue.offer`): if `Enqueued` (happiest path) dispatches the associated result to the
    * inner route, if `Dropped` (not so happy path) completes the HTTP request with
    * `ServiceUnavailable` and else (failure case, should not happen) runs the coordinated shutdown
    * with the reason [[ProcessorCompletedOrFailed]] and completes the HTTP request with
    * `InternalServerError`.
    *
    * @param processor the processor to work with
    * @param timeout maximum duration for the command to be processed, i.e. the related promise to be completed
    * @param shutdown Akka Coordinated Shutdown
    * @param c command to be processed
    * @param inner inner route
    * @param ec Scala execution context for timeout handling
    * @param scheduler Akka scheduler needed for timeout handling
    * @tparam C command type
    * @tparam R result type
    */
  def onProcessorSuccess[C, R](
      processor: SourceQueue[(C, Promise[R])],
      timeout: FiniteDuration,
      shutdown: CoordinatedShutdown
  )(c: C)(inner: R => Route)(implicit ec: ExecutionContext, scheduler: Scheduler): Route = {
    val result = ExpiringPromise[R](timeout)
    onSuccess(processor.offer((c, result))) {
      case Enqueued =>
        logger.debug(s"Successfully enqueued command $c!")
        onSuccess(result.future)(inner)

      case Dropped =>
        logger.warn(s"Dropped command $c!")
        complete(StatusCodes.ServiceUnavailable)

      case _ =>
        logger.error(s"Shutting down, because Processor completed or failed!")
        shutdown.run(ProcessorCompletedOrFailed)
        complete(StatusCodes.InternalServerError)
    }
  }
}
