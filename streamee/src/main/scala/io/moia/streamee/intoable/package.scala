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
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.stream.{ KillSwitches, Materializer, UniqueKillSwitch }
import akka.stream.scaladsl.{ Flow, Keep, MergeHub, Sink, Source, FlowOps => AkkaFlowOps }
import akka.util.Timeout
import akka.Done
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration.FiniteDuration

package object intoable {

  implicit final class SourceOps[A, M](val source: Source[A, M]) extends AnyVal {

    def into[B](
        intoableSink: Sink[(A, Promise[B]), Any],
        responseTimeout: FiniteDuration,
        parallelism: Int
    )(implicit ec: ExecutionContext, scheduler: Scheduler): Source[B, M] =
      intoImpl(source, intoableSink, responseTimeout, parallelism)

    def intoRemote[B](
        intoableSink: Sink[(A, ActorRef[Respondee.Command[B]]), Any],
        responseTimeout: FiniteDuration,
        parallelism: Int
    )(implicit ec: ExecutionContext,
      scheduler: Scheduler,
      respondeeFactory: ActorRef[RespondeeFactory.Command[B]]): Source[B, M] =
      intoRemoteImpl(source, intoableSink, responseTimeout, parallelism)
  }

  implicit final class FlowOps[A, B, M](val flow: Flow[A, B, M]) extends AnyVal {
    def into[C](
        intoableSink: Sink[(B, Promise[C]), Any],
        responseTimeout: FiniteDuration,
        parallelism: Int
    )(implicit ec: ExecutionContext, scheduler: Scheduler): Flow[A, C, M] =
      intoImpl(flow, intoableSink, responseTimeout, parallelism)
  }

  private def intoImpl[A, B, M](
      flowOps: AkkaFlowOps[A, M],
      intoableSink: Sink[(A, Promise[B]), Any],
      responseTimeout: FiniteDuration,
      parallelism: Int
  )(implicit ec: ExecutionContext, scheduler: Scheduler): flowOps.Repr[B] =
    flowOps
      .map(a => (a, ExpiringPromise[B](responseTimeout)))
      .alsoTo(intoableSink)
      .mapAsync(parallelism)(_._2.future)

  private def intoRemoteImpl[A, B, M](
      flowOps: AkkaFlowOps[A, M],
      intoableSink: Sink[(A, ActorRef[Respondee.Command[B]]), Any],
      responseTimeout: FiniteDuration,
      parallelism: Int
  )(implicit ec: ExecutionContext,
    scheduler: Scheduler,
    respondeeFactory: ActorRef[RespondeeFactory.Command[B]]): flowOps.Repr[B] = {
    implicit val askTimeout: Timeout = responseTimeout
    flowOps
      .mapAsync(parallelism) { a =>
        val b = Promise[B]()
        def createRespondee(replyTo: ActorRef[RespondeeFactory.RespondeeCreated[B]]) =
          RespondeeFactory.CreateRespondee[B](b, responseTimeout, replyTo)
        val respondee = (respondeeFactory ? createRespondee).map(_.respondee)
        Future.successful((a, b)).zip(respondee)
      }
      .alsoTo(
        Flow[((A, Promise[B]), ActorRef[Respondee.Command[B]])]
          .map { case ((a, _), r) => (a, r) }
          .to(intoableSink)
      )
      .mapAsync(parallelism)(_._1._2.future)
  }

  def runIntoableFlow[A, B](intoableFlow: Flow[(A, Promise[B]), (B, Promise[B]), Any],
                            bufferSize: Int)(
      implicit mat: Materializer
  ): (Sink[(A, Promise[B]), Any], Future[Done]) =
    MergeHub
      .source[(A, Promise[B])](bufferSize)
      .via(intoableFlow)
      .toMat(Sink.foreach { case (b, p) => p.trySuccess(b) })(Keep.both)
      .run()

  def runRemotelyIntoableFlow[A, B](intoableFlow: Flow[(A, ActorRef[Respondee.Command[B]]),
                                                       (B, ActorRef[Respondee.Command[B]]),
                                                       Any],
                                    bufferSize: Int)(
      implicit mat: Materializer
  ): (Sink[(A, ActorRef[Respondee.Command[B]]), Any], UniqueKillSwitch, Future[Done]) =
    MergeHub
      .source[(A, ActorRef[Respondee.Command[B]])](bufferSize)
      .viaMat(KillSwitches.single)(Keep.both)
      .via(intoableFlow)
      .toMat(Sink.foreach { case (b, r) => r ! Respondee.Response(b) }) {
        case ((s, r), d) => (s, r, d)
      }
      .run()
}
