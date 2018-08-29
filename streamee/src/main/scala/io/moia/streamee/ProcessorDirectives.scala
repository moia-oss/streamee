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
    * given `timeout` and handles the returned `OfferQueueResult` (from Akka Streams
    * `SourceQueue.offer`): if `Enqueued` (happiest path) dispatches the associated result to the
    * inner route via `onSuccess`, if `Dropped` (not so happy path) completes the HTTP request with
    * `ServiceUnavailable` and else (failure case, should not happen) completes the HTTP request
    * with `InternalServerError`.
    *
    * @param command command to be processed
    * @param processor the processor to work with
    * @param timeout maximum duration for the command to be processed, i.e. the related promise to be completed
    * @param scheduler Akka scheduler needed for timeout handling
    * @tparam C command type
    * @tparam R result type
    */
  def onProcessorSuccess[C, R](command: C,
                               processor: SourceQueue[(C, Promise[R])],
                               timeout: FiniteDuration,
                               scheduler: Scheduler): Directive1[R] =
    extractExecutionContext.flatMap { implicit ec =>
      val result = ExpiringPromise[R](timeout, scheduler)

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
