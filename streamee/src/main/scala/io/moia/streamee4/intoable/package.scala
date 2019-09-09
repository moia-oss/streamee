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
import akka.stream.{ FlowShape, KillSwitch, KillSwitches, Materializer }
import akka.stream.scaladsl.{
  Flow,
  FlowWithContext,
  GraphDSL,
  Keep,
  MergeHub,
  Sink,
  Unzip,
  Zip,
  FlowOps => AkkaFlowOps
}
import akka.util.Timeout
import akka.Done
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration.FiniteDuration

package object intoable {

  /**
    * A sink that can be used with the `into` extension method for [[Process]]es.
    *
    * @tparam Req request type
    * @tparam Res response type
    */
  type IntoableSink[Req, Res] = Sink[(Req, Promise[Res]), Any]

  /**
    * A sink that can be used with the `into` extension method for [[RemotelyIntoableProcess]]es.
    *
    * @tparam Req request type
    * @tparam Res response type
    */
  type RemotelyIntoableSink[Req, Res] = Sink[(Req, Respondee[Res]), Any]

  /**
    * A domain logic process from request to response which transparently propagates a [[Respondee]]
    * for the response .
    *
    * @tparam Req request type
    * @tparam Res response type
    */
  type RemotelyIntoableProcess[-Req, Res] = RemotelyIntoableProcessStage[Req, Res, Res]

  /**
    * A part of a [[RemotelyIntoableProcess]].
    *
    * @tparam In input type
    * @tparam Out output type
    * @tparam Res overall process response type
    */
  type RemotelyIntoableProcessStage[-In, Out, Res] =
    FlowWithContext[Respondee[Res], In, Respondee[Res], Out, Any]

  type Respondee[Res] = ActorRef[Respondee.Response[Res]]

  type RespondeeFactory[Res] = ActorRef[RespondeeFactory.CreateRespondee[Res]]

  /**
    * Extension methods for `FlowWithContext`s.
    */
  implicit final class FlowWithContextOps[CtxIn, In, CtxOut, Out](
      val flowWithContext: FlowWithContext[CtxIn, In, CtxOut, Out, Any]
  ) extends AnyVal {

    /**
      * Emit into the given [[IntoableSink]] and continue with its responses.
      */
    def into[Out2](
        intoableSink: IntoableSink[Out, Out2],
        parallelism: Int
    )(implicit ec: ExecutionContext,
      scheduler: Scheduler): FlowWithContext[CtxIn, In, CtxOut, Out2, Any] =
      flowWithContext.via(
        Flow.fromGraph(
          GraphDSL.create() { implicit builder =>
            import GraphDSL.Implicits._

            val unzip = builder.add(Unzip[Out, CtxOut]())
            val into = builder.add(
              Flow[Out]
                .map(in => (in, Promise[Out2]()))
                .alsoTo(intoableSink)
                .mapAsync(parallelism)(_._2.future)
            )
            val zip = builder.add(Zip[Out2, CtxOut])

            // format: off
            unzip.out0 ~> into ~> zip.in0
            unzip.out1     ~>     zip.in1
            // format: on

            FlowShape(unzip.in, zip.out)
          }
        )
      )

    /**
      * Emit into the given [[RemotelyIntoableSink]] and continue with its responses.
      */
    def into[Out2](
        remotelyIntoableSink: RemotelyIntoableSink[Out, Out2],
        responseTimeout: FiniteDuration,
        parallelism: Int
    )(implicit ec: ExecutionContext,
      scheduler: Scheduler,
      respondeeFactory: RespondeeFactory[Out2]): FlowWithContext[CtxIn, In, CtxOut, Out2, Any] =
      flowWithContext.via(
        Flow.fromGraph(
          GraphDSL.create() {
            implicit builder =>
              import GraphDSL.Implicits._

              val unzip = builder.add(Unzip[Out, CtxOut]())
              val into =
                builder.add(
                  Flow[Out]
                    .mapAsync(parallelism) { out =>
                      implicit val askTimeout: Timeout = responseTimeout
                      val out2                         = Promise[Out2]()
                      def crtRespondee(replyTo: ActorRef[RespondeeFactory.RespondeeCreated[Out2]]) =
                        RespondeeFactory.CreateRespondee[Out2](out2,
                                                               responseTimeout,
                                                               replyTo,
                                                               s"a = $out")
                      val respondee = (respondeeFactory ? crtRespondee).map(_.respondee)
                      Future.successful((out, out2)).zip(respondee)
                    }
                    .alsoTo(
                      Flow[((Out, Promise[Out2]), Respondee[Out2])]
                        .map { case ((out, _), respondee) => (out, respondee) }
                        .to(remotelyIntoableSink)
                    )
                    .mapAsync(parallelism) { case ((_, out2), _) => out2.future }
                )
              val zip = builder.add(Zip[Out2, CtxOut])

              // format: off
              unzip.out0 ~> into ~> zip.in0
              unzip.out1     ~>     zip.in1
              // format: on

              FlowShape(unzip.in, zip.out)
          }
        )
      )
  }

  /**
    * Run the given `process` with the given `bufferSize` (must be positive).
    *
    * @return intoable sink to be used with `into` and completion signal
    */
  def runProcess[A, B](process: Process[A, B], bufferSize: Int)(
      implicit mat: Materializer
  ): (IntoableSink[A, B], Future[Done]) =
    MergeHub
      .source[(A, Promise[B])](bufferSize)
      .via(process)
      .toMat(Sink.foreach { case (b, p) => p.trySuccess(b) })(Keep.both)
      .run()

  /**
    * Run the given `remotelyIntoableProcess` with the given `bufferSize` (must be positive). Notice
    * that using the returned kill switch might result in dropping (loosing) `bufferSize` number of
    * elements!
    *
    * @return remotely intoable sink to be used with `into`, kill switch and completion signal
    *         (which should not happen except for using the kill switch)
    */
  def runRemotelyIntoableProcess[A, B](
      remotelyIntoableProcess: RemotelyIntoableProcess[A, B],
      bufferSize: Int
  )(implicit mat: Materializer): (RemotelyIntoableSink[A, B], KillSwitch, Future[Done]) =
    MergeHub
      .source[(A, Respondee[B])](bufferSize)
      .viaMat(KillSwitches.single)(Keep.both)
      .via(remotelyIntoableProcess)
      .toMat(Sink.foreach { case (b, r) => r ! Respondee.Response(b) }) {
        case ((s, r), d) => (s, r, d)
      }
      .run()

  private def intoImpl[A, B, M](
      flowOps: AkkaFlowOps[A, M],
      intoableSink: IntoableSink[A, B],
      parallelism: Int
  )(implicit ec: ExecutionContext, scheduler: Scheduler): flowOps.Repr[B] =
    flowOps
      .map(a => (a, Promise[B]()))
      .alsoTo(intoableSink)
      .mapAsync(parallelism)(_._2.future)

  private def intoImpl[A, B, M](
      flowOps: AkkaFlowOps[A, M],
      remotelyIntoableSink: RemotelyIntoableSink[A, B],
      responseTimeout: FiniteDuration,
      parallelism: Int
  )(implicit ec: ExecutionContext,
    scheduler: Scheduler,
    respondeeFactory: RespondeeFactory[B]): flowOps.Repr[B] =
    flowOps
      .mapAsync(parallelism) { a =>
        implicit val askTimeout: Timeout = responseTimeout // let's use the same timeout
        val b                            = Promise[B]()
        def createRespondee(replyTo: ActorRef[RespondeeFactory.RespondeeCreated[B]]) =
          RespondeeFactory.CreateRespondee[B](b, responseTimeout, replyTo, s"a = $a")
        val respondee = (respondeeFactory ? createRespondee).map(_.respondee)
        Future.successful((a, b)).zip(respondee)
      }
      .alsoTo(
        Flow[((A, Promise[B]), Respondee[B])]
          .map { case ((a, _), r) => (a, r) }
          .to(remotelyIntoableSink)
      )
      .mapAsync(parallelism) { case ((_, b), _) => b.future }
}
