package io.moia.streamee

import akka.actor.Scheduler
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Directives.{ complete, extractExecutionContext, onSuccess }
import akka.stream.QueueOfferResult
import akka.stream.scaladsl.SourceQueue
import org.apache.logging.log4j.scala.Logging
import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration

/**
  * Directives to be used with processors.
  */
object ProcessorDirectives extends Logging {

  /**
    * Offers the given command to the given processor thereby using an [[ExpiringPromise]] with the
    * given `maxLatency` and handles the returned `OfferQueueResult` (from Akka Streams
    * `SourceQueue.offer`): if `Enqueued` (happiest path) dispatches the associated result to the
    * inner route via `onSuccess`, if `Dropped` (not so happy path) completes the HTTP request with
    * `ServiceUnavailable` and else (failure case, should not happen) completes the HTTP request
    * with `InternalServerError`.
    *
    * @param command command to be processed
    * @param processor the processor to work with
    * @param maxLatency maximum duration for the command to be processed, i.e. the related promise to be completed
    * @param scheduler Akka scheduler needed for timeout handling
    * @tparam C command type
    * @tparam R result type
    */
  def onProcessorSuccess[C, R](command: C,
                               processor: SourceQueue[(C, Promise[R])],
                               maxLatency: FiniteDuration,
                               scheduler: Scheduler): Directive1[R] =
    extractExecutionContext.flatMap { implicit ec =>
      val result = ExpiringPromise[R](maxLatency, scheduler)

      onSuccess(processor.offer((command, result))).flatMap {
        case QueueOfferResult.Enqueued =>
          logger.debug(s"Successfully enqueued command $command!")
          onSuccess(result.future)

        case QueueOfferResult.Dropped =>
          logger.warn(s"Processor dropped command $command!")
          complete(StatusCodes.ServiceUnavailable)

        case QueueOfferResult.QueueClosed =>
          logger.error(s"Processor completed unexpectedly!")
          complete(StatusCodes.InternalServerError)

        case QueueOfferResult.Failure(cause) =>
          logger.error(s"Processor failed unexpectedly!", cause)
          complete(StatusCodes.InternalServerError)
      }
    }
}
