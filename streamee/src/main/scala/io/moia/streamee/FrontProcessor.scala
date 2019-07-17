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

import akka.stream.{
  ActorAttributes,
  Materializer,
  OverflowStrategy,
  QueueOfferResult,
  StreamDetachedException,
  Supervision
}
import akka.stream.scaladsl.{ Keep, Sink, Source }
import akka.Done
import akka.stream.QueueOfferResult.{ Dropped, Enqueued }
import org.apache.logging.log4j.scala.Logging
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.{ Duration, FiniteDuration }

object FrontProcessor {

  /**
    * Signals that a request cannot be handled at this time.
    *
    * @param name name of the processor
    */
  final case class ProcessorUnavailable(name: String)
      extends Exception(s"Processor $name cannot accept requests at this time!")

  /**
    * Signals an unexpected result of calling [[FrontProcessor.accept]].
    *
    * @param cause the underlying erroneous `QueueOfferResult`, e.g. `Failure` or `QueueClosed`
    */
  final case class ProcessorError(cause: QueueOfferResult)
      extends Exception(s"QueueOfferResult $cause was not expected!")

  /**
    * See [[FrontProcessor]].
    */
  def apply[Req, Res](
      process: Process[Req, Res, Res],
      timeout: FiniteDuration,
      name: String,
      bufferSize: Int = 1
  )(implicit mat: Materializer, ec: ExecutionContext): FrontProcessor[Req, Res] =
    new FrontProcessor(process, timeout, name, bufferSize)
}

/**
  * Run a `Source.queue` for pairs of request and [[Respondee]] via the given `process` to a
  * `Sink` responding to the [[Respondee]].
  *
  * When [[accept]] is called, the given request is emitted into the process. The
  * returned `Future` is either completed successfully with the response or failed if the process
  * back-pressures or does not create the response in time.
  *
  * @param process    top-level domain logic process from request to response
  * @param timeout    maximum duration for the running process to respond; must be positive!
  * @param name       name, used for logging and exceptions
  * @param bufferSize optional size of the buffer of the used `MergeHub.source`; defaults to 1; must be positive!
  * @tparam Req request type
  * @tparam Res response type
  */
final class FrontProcessor[Req, Res] private (
    process: Process[Req, Res, Res],
    timeout: FiniteDuration,
    name: String,
    bufferSize: Int = 1
)(implicit mat: Materializer, ec: ExecutionContext)
    extends Logging {
  import FrontProcessor._

  require(timeout > Duration.Zero, s"timeout for processor $name must be > 0, but was $timeout!")
  require(bufferSize > 0, s"bufferSize for processor $name must be > 0, but was $bufferSize!")

  private val (queue, _done) =
    Source
      .queue[(Req, Respondee[Res])](bufferSize, OverflowStrategy.dropNew)
      .via(process)
      .toMat(
        Sink.foreach { case (response, respondee) => respondee ! Respondee.Response(response) }
      )(Keep.both)
      .withAttributes(ActorAttributes.supervisionStrategy(resume))
      .run()

  /**
    * Ingest the given request into the process. The returned `Future` is either completed
    * successfully with the response or failed with [[ProcessorUnavailable]], if the process
    * back-pressures or with [[TimeoutException]], if the response is not produced in time.
    *
    * @param request request to be accepted
    * @return `Future` for the response
    */
  def accept(request: Req): Future[Res] = {
    val (respondee, response) = Respondee.spawn[Res](timeout)
    queue
      .offer((request, respondee))
      .recover { case _: StreamDetachedException => Dropped } // after shutdown we want to fail with `ProcessorUnavailable`
      .flatMap {
        case Enqueued => response.future // might result in a `TimeoutException`
        case Dropped  => Future.failed(ProcessorUnavailable(name))
        case other    => Future.failed(ProcessorError(other))
      }
  }

  /**
    * Shutdown this processor. Already accepted requests are completed, but no new ones are
    * accepted. To watch shutdown completion use [[whenDone]].
    */
  def shutdown(): Unit = {
    logger.warn(s"Shutdown for processor $name requested!")
    queue.complete()
  }

  /**
    * The returned `Future` is completed when the running process is completed, e.g. via
    * [[shutdown]] or unexpected failure.
    *
    * @return signal for completion
    */
  def whenDone: Future[Done] =
    _done

  private def resume(cause: Throwable) = {
    logger.error(s"Processor $name failed and resumes", cause)
    Supervision.Resume
  }
}
