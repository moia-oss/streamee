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
import akka.actor.CoordinatedShutdown
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
    * Signals an unexpected result of calling [[FrontProcessor.offer]].
    *
    * @param cause the underlying erroneous `QueueOfferResult`, e.g. `Failure` or `QueueClosed`
    */
  final case class ProcessorError(cause: QueueOfferResult)
      extends Exception(s"QueueOfferResult $cause was not expected!")

  /**
    * Create a [[FrontProcessor]]: run a `Source.queue` for pairs of request and [[Respondee]] via
    * the given `process` to a `Sink` responding to the [[Respondee]].
    *
    * When [[FrontProcessor.offer]] is called, the given request is emitted into the process. The
    * returned `Future` is either completed successfully with the response or failed if the process
    * back-pressures or does not create the response in time.
    *
    * @param process top-level domain logic process from request to response
    * @param timeout maximum duration for the running process to respond; must be positive!
    * @param name name, used for logging and exceptions
    * @param bufferSize optional size of the buffer of the used `Source.queue`; defaults to 1; must
    *                   be positive!
    * @param phase identifier for a phase of `CoordinatedShutdown`; defaults to
    *              "service-requests-done"; must be defined in configufation!
    * @tparam Req request type
    * @tparam Res response type
    * @return [[FrontProcessor]]
    */
  def apply[Req, Res](
      process: Process[Req, Res, Res],
      timeout: FiniteDuration,
      name: String,
      bufferSize: Int = 1,
      phase: String = CoordinatedShutdown.PhaseServiceRequestsDone
  )(implicit mat: Materializer, ec: ExecutionContext): FrontProcessor[Req, Res] =
    new FrontProcessor(process, timeout, name, bufferSize, phase)
}

/**
  * Run a `Source.queue` for pairs of request and [[Respondee]] via the given `process` to a `Sink`
  * responding to the [[Respondee]].
  */
final class FrontProcessor[Req, Res] private (
    process: Process[Req, Res, Res],
    timeout: FiniteDuration,
    name: String,
    bufferSize: Int,
    phase: String
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
      .addAttributes(ActorAttributes.supervisionStrategy(resume))
      .run()

  CoordinatedShutdown(mat.system).addTask(phase, s"shutdown-front-processor-$name") { () =>
    shutdown()
    whenDone
  }

  /**
    * Offer the given request to the process. The returned `Future` is either completed successfully
    * with the response or failed with [[FrontProcessor.ProcessorUnavailable]], if the process
    * back-pressures or with [[ResponseTimeoutException]], if the response is not produced in time.
    *
    * @param request request to be offered
    * @return eventual response
    */
  def offer(request: Req): Future[Res] = {
    val (respondee, response) = Respondee.spawn[Res](timeout)
    queue
      .offer((request, respondee))
      .recover { case _: StreamDetachedException => Dropped } // after shutdown fail with // `ProcessorUnavailable`
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
    * @return completion signal
    */
  def whenDone: Future[Done] =
    _done

  private def resume(cause: Throwable) = {
    logger.error(s"Processor $name failed and resumes", cause)
    Supervision.Resume
  }
}
