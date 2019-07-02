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
import akka.stream.{
  ActorAttributes,
  ActorMaterializer,
  KillSwitches,
  Materializer,
  OverflowStrategy,
  QueueOfferResult,
  Supervision
}
import akka.stream.scaladsl.{ Keep, MergeHub, Sink, Source }
import akka.Done
import akka.actor.typed.scaladsl.adapter.UntypedActorSystemOps
import akka.stream.QueueOfferResult.{ Dropped, Enqueued }
import org.apache.logging.log4j.scala.Logging
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration.{ Duration, FiniteDuration }

/**
  * Factories and utilities for [[Processor]]s.
  */
object Processor extends Logging {

  /**
    * Signals that a request cannot be handled at this time.
    *
    * @param name name of the processor
    */
  final case class ProcessorUnavailable(name: String)
      extends Exception(s"Processor $name cannot accept requests at this time!")

  /**
    * Signals an unexpected result of calling [[Processor.accept]].
    *
    * @param cause the underlying erroneous `QueueOfferResult`, e.g. `Failure` or `QueueClosed`
    */
  final case class ProcessorError(cause: QueueOfferResult)
      extends Exception(s"QueueOfferResult $cause was not expected!")

  /**
    * Run a `Source.queue` for pairs of request and [[Respondee]] via the given `process` to a
    * `Sink` responding to the [[Respondee]] and return a [[Processor]].
    *
    * When [[Processor.accept]] is called, the given request is emitted into the process given here.
    * The returned `Future` is either completed successfully with the response or failed if the
    * process back-pressures or does not create the response in time.
    *
    * @param process    top-level domain logic process from request to response
    * @param timeout    maximum duration for the running process to respond; must be positive!
    * @param name       name, used e.g. in [[Processor.ProcessorUnavailable]] exceptions
    * @param bufferSize optional size of the buffer of the used `MergeHub.source`; defaults to 1; must be positive!
    * @tparam Req request type
    * @tparam Res response type
    * @return [[Processor]] for processing requests and shutting down
    */
  def runToProcessor[Req, Res](process: Process[Req, Res, Res],
                               timeout: FiniteDuration,
                               name: String,
                               bufferSize: Int = 1,
  )(implicit mat: Materializer, ec: ExecutionContext, scheduler: Scheduler): Processor[Req, Res] = {
    require(
      timeout > Duration.Zero,
      s"timeout must be positive, but was $timeout!"
    )
    require(
      bufferSize > 0,
      s"bufferSize must be positive, but was $bufferSize!"
    )

    val (queue, done) =
      Source
        .queue[(Req, Respondee[Res])](bufferSize, OverflowStrategy.dropNew)
        .via(process)
        .toMat(Sink.foreach {
          case (response, respondee) => respondee ! Respondee.Response(response)
        })(Keep.both)
        .withAttributes(ActorAttributes.supervisionStrategy(resume(name)))
        .run()

    new Processor[Req, Res] {

      override def accept(request: Req): Future[Res] = {
        val (response, respondee) = createRespondee[Res](mat, timeout)
        queue.offer((request, respondee)).flatMap {
          case Enqueued => response.future
          case Dropped  => Future.failed(ProcessorUnavailable(name))
          case other    => Future.failed(ProcessorError(other))
        }
      }

      override def shutdown(): Future[Done] = {
        queue.complete()
        done
      }
    }
  }

  /**
    * Run a `MergeHub.source` for pairs of request and [[Respondee]] via the given `process` to a
    * `Sink` responding to the [[Respondee]] and return an [[ProcessSink]], a `KillSwitch` and a
    * completion signal. Notice that using the returned kill switch might result in dropping
    * (losing) `bufferSize` number of requsts!
    *
    * @param process top-level domain logic process from request to response
    * @param timeout    maximum duration for the running process to respond; must be positive!
    * @param name       name, used e.g. in [[Processor.ProcessorUnavailable]] exceptions
    * @param bufferSize optional size of the buffer of the used `MergeHub.source`; defaults to 1; must be positive!
    * @tparam Req request type
    * @tparam Res response type
    * @return [[ProcessSink]] to be used with `into`, kill switch and completion signal (which should not happen except for using the kill switch)
    */
  def runToIntoableSink[Req, Res](process: Process[Req, Res, Res],
                                  timeout: FiniteDuration,
                                  name: String,
                                  bufferSize: Int = 1)(
      implicit mat: Materializer
  ): Processor[Req, Res] = {
    require(
      bufferSize > 0,
      s"bufferSize must be positive, but was $bufferSize!"
    )

    val (sink, switch, done) =
      MergeHub
        .source[(Req, Respondee[Res])](bufferSize)
        .viaMat(KillSwitches.single)(Keep.both)
        .via(process)
        .toMat(Sink.foreach {
          case (response, respondee) => respondee ! Respondee.Response(response)
        }) { case ((sink, switch), done) => (sink, switch, done) }
        .withAttributes(ActorAttributes.supervisionStrategy(resume(name)))
        .run()

    new Processor[Req, Res] {

      override def accept(request: Req): Future[Res] = {
        val (response, respondee) = createRespondee[Res](mat, timeout)
        Source.single((request, respondee)).runWith(sink)
        response.future
      }

      override def shutdown(): Future[Done] = {
        switch.shutdown()
        done
      }
    }
  }

  private def resume(name: String)(cause: Throwable) = {
    logger.error(s"Processor $name failed and resumes", cause)
    Supervision.Resume
  }

  private def createRespondee[A](mat: Materializer, timeout: FiniteDuration) = {
    val response = Promise[A]()
    val respondee =
      mat
        .asInstanceOf[ActorMaterializer]
        .system
        .spawnAnonymous(Respondee[A](response, timeout))
    (response, respondee)
  }
}

/**
  * Offers interactions with a running process: accept requests and shutdown.
  */
trait Processor[Req, Res] {

  /**
    * Accept a request for processing.
    *
    * @param request request to be processed
    * @return `Future` for the response
    */
  def accept(request: Req): Future[Res]

  /**
    * Shut down the running process of this [[Processor]]. Already accepted requests should still
    * be handled, but new ones should no longer be accepted.
    *
    * @return `Future` signaling that shutdown has completed
    */
  def shutdown(): Future[Done]
}
