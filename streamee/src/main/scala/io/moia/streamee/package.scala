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

package io.moia

import akka.actor.typed.ActorRef
import akka.actor.Scheduler
import akka.stream.scaladsl.{ Flow, FlowWithContext, Sink }
import akka.NotUsed
import akka.actor.typed.scaladsl.adapter.UntypedActorSystemOps
import akka.stream.DelayOverflowStrategy
import scala.concurrent.{ ExecutionContext, Promise }
import scala.concurrent.duration.FiniteDuration

package object streamee {

  type Respondee[A] = ActorRef[Respondee.Response[A]]

  /**
    * A domain logic process or process stage from an input to an output which transparently
    * propagates a [[Respondee]] for the top-level response. For a top-level process Out == Res. Can
    * be used locally or remotely.
    *
    * @tparam In input type
    * @tparam Out output type
    * @tparam Res response type
    */
  type Process[-In, Out, Res] = FlowWithContext[In, Respondee[Res], Out, Respondee[Res], Any]

  type IntoableSink[Req, Res] = Sink[(Req, Respondee[Res]), Any]

  /**
    * Extension methods for [[Process]]es.
    */
  implicit final class ProcessOps[In, Out, Res](val process: Process[In, Out, Res]) extends AnyVal {

    /**
      * Emit into the given "intoable" `Sink` and continue with its response.
      *
      * @param sink "intoable" sink from [[Process.runToIntoableSink]]
      * @param parallelism maximum number of elements which are currently in-flight in the given sink
      * @param timeout maximum duration for the sink to respond
      */
    def into[Out2](
        sink: Sink[(Out, Respondee[Out2]), NotUsed],
        parallelism: Int,
        timeout: FiniteDuration
    )(implicit ec: ExecutionContext, scheduler: Scheduler): Process[In, Out2, Res] =
      process
        .via(Flow.setup { (mat, _) => // Get access to the `ActorMaterializer` via `Flow.setup`
          Flow.apply.map { case (out, respondee) => ((out, mat), respondee) }
        })
        .map { // Create respondee via the `ActorMaterializer`
          case (out, mat) =>
            val promisedOut2 = Promise[Out2]()
            val respondee: Respondee[Out2] =
              mat.system.spawnAnonymous(Respondee[Out2](promisedOut2, timeout))
            (out, promisedOut2, respondee)
        }
        .via(Flow.apply.alsoTo {
          Flow[((Out, Promise[Out2], Respondee[Out2]), Respondee[Res])]
            .map { case ((out, _, respondee), _) => (out, respondee) }
            .to(sink)
        })
        .mapAsync(parallelism) { case (_, promisedOut2, _) => promisedOut2.future }
  }

  /**
    * Extension methods for `FlowWithContext`.
    */
  implicit final class FlowWithContextExt[In, CtxIn, Out, CtxOut](
      val flowWithContext: FlowWithContext[In, CtxIn, Out, CtxOut, Any]
  ) extends AnyVal {

    def pushIn: FlowWithContext[In, CtxIn, Out, (Out, CtxOut), Any] =
      flowWithContext.via(Flow.apply.map { case (out, ctxOut) => (out, (out, ctxOut)) })
  }

  /**
    * Extension methods for `FlowWithContext` with paired output context (see
    * [[FlowWithContextExt.pushIn]]).
    */
  implicit final class FlowWithContextExt2[In, CtxIn, Out, CtxOut, In0](
      val flowWithContext: FlowWithContext[In, CtxIn, Out, (In0, CtxOut), Any]
  ) extends AnyVal {

    def popIn: FlowWithContext[In, CtxIn, (In0, Out), CtxOut, Any] =
      flowWithContext.via(Flow.apply.map { case (out, (in0, ctxOut)) => ((in0, out), ctxOut) })
  }

  /**
    * Missing standard operators from `FlowOps` not yet defined on `FlowWithContext` (by Akka).
    */
  implicit final class FlowWithContextExtAkka[In, CtxIn, Out, CtxOut](
      val flowWithContext: FlowWithContext[In, CtxIn, Out, CtxOut, Any]
  ) extends AnyVal {

    def delay(
        of: FiniteDuration,
        strategy: DelayOverflowStrategy = DelayOverflowStrategy.dropTail
    ): FlowWithContext[In, CtxIn, Out, CtxOut, Any] =
      flowWithContext.via(Flow.apply.delay(of, strategy))
  }
}
