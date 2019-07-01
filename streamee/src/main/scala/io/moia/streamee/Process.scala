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
  KillSwitches,
  Materializer,
  OverflowStrategy,
  Supervision,
  UniqueKillSwitch
}
import akka.stream.scaladsl.{ FlowWithContext, Keep, MergeHub, Sink, Source }
import akka.Done
import org.apache.logging.log4j.scala.Logging
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.{ Duration, FiniteDuration }

object Process extends Logging {

  def apply[Req, Res](): Process[Req, Req, Res] =
    FlowWithContext[Req, Respondee[Res]]

  /**
    * Run a `Source.queue` for pairs of request and [[Respondee]] via the given `process` to a
    * `Sink` responding to the [[Respondee]] and return a [[Handler]].
    *
    * When [[Handler.handle]] is called, the given request is emitted into the process given here.
    * The returned `Future` is either completed successfully with the response or failed if the
    * process back-pressures or does not create the response in time.
    *
    * @param process    top-level domain logic process from request to response
    * @param timeout    maximum duration for the running process to respond; must be positive!
    * @param bufferSize size of the buffer of the input queue; must be positive!
    * @param name       name, used e.g. in [[Handler.ProcessUnavailable]] exceptions
    * @tparam Req request type
    * @tparam Res response type
    * @return [[Handler]] for processing requests and shutting down
    */
  def runToHandler[Req, Res](
      process: Process[Req, Res, Res],
      timeout: FiniteDuration,
      bufferSize: Int,
      name: String
  )(implicit mat: Materializer, ec: ExecutionContext, scheduler: Scheduler): Handler[Req, Res] = {
    require(timeout > Duration.Zero, s"timeout must be positive, but was $timeout!")
    require(bufferSize > 0, s"bufferSize must be positive, but was $bufferSize!")

    val (queue, done) =
      Source
        .queue[(Req, Respondee[Res])](bufferSize, OverflowStrategy.dropNew)
        .via(process)
        .toMat(Sink.foreach {
          case (response, respondee) => respondee ! Respondee.Response(response)
        })(Keep.both)
        .withAttributes(ActorAttributes.supervisionStrategy(resume(name)))
        .run()

    new Handler[Req, Res](queue, done, timeout, name)
  }

  /**
    * Run a `MergeHub.source` for pairs of request and [[Respondee]] via the given `process` to a
    * `Sink` responding to the [[Respondee]] and return an [[IntoableSink]], a `KillSwitch` and a
    * completion signal. Notice that using the returned kill switch might result in dropping
    * (losing) `bufferSize` number of requsts!
    *
    * @param process top-level domain logic process from request to response
    * @param bufferSize optional size of the buffer of the used `MergeHub.source`; defaults to 1; must be positive!
    * @tparam Req request type
    * @tparam Res response type
    * @return [[IntoableSink]] to be used with `into`, kill switch and completion signal (which should not happen except for using the kill switch)
    */
  def runToIntoableSink[Req, Res](process: Process[Req, Res, Res], bufferSize: Int = 1)(
      implicit mat: Materializer
  ): (IntoableSink[Req, Res], UniqueKillSwitch, Future[Done]) = {
    require(bufferSize > 0, s"bufferSize must be positive, but was $bufferSize!")

    MergeHub
      .source[(Req, Respondee[Res])](bufferSize)
      .viaMat(KillSwitches.single)(Keep.both)
      .via(process)
      .toMat(
        Sink.foreach { case (response, respondee) => respondee ! Respondee.Response(response) }
      ) { case ((sink, switch), done) => (sink, switch, done) }
      .run()
  }

  private def resume(name: String)(cause: Throwable) = {
    logger.error(s"Processor $name failed and resumes", cause)
    Supervision.Resume
  }
}
