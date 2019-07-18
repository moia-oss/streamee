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
import akka.stream.{ DelayOverflowStrategy, SinkRef, ThrottleMode }
import akka.stream.scaladsl.{ Flow, FlowWithContext, Sink, Source }
import scala.concurrent.duration.{ Duration, FiniteDuration }
import scala.concurrent.{ Future, Promise }

package object streamee {

  /**
    * A domain logic process or process stage from an input to an output which transparently
    * propagates a [[Respondee]] for the top-level response. For a top-level process Out == Res. Can
    * be used locally or remotely.
    */
  type Process[-In, Out, Res] = FlowWithContext[In, Respondee[Res], Out, Respondee[Res], Any]

  /**
    * Convenient shortcut for `ActorRef[Respondee.Response[A]]`.
    */
  type Respondee[A] = ActorRef[Respondee.Response[A]]

  /**
    * Convenient shortcut for `Sink[(Req, Respondee[Res]), Any]`.
    */
  type ProcessSink[Req, Res] = Sink[(Req, Respondee[Res]), Any]

  /**
    * Convenient shortcut for `SinkRef[(Req, Respondee[Res])]`.
    */
  type ProcessSinkRef[Req, Res] = SinkRef[(Req, Respondee[Res])]

  /**
    * Signals no response within the given timeout.
    *
    * @param timeout maximum duration for the response
    */
  final case class ResponseTimeoutException(timeout: FiniteDuration)
      extends Exception(s"No response within $timeout!")

  /**
    * Extension methods for `Source`.
    */
  implicit final class SourceExt[Out, M](val source: Source[Out, M]) extends AnyVal {

    /**
      * Ingest into the given [[ProcessSink]] and emit its response.
      *
      * @param processSink [[ProcessSink]] to emit into
      * @param timeout maximum duration for the running process to respond; must be positive!
      * @tparam Out2 response type of the given [[ProcessSink]]
      */
    def into[Out2](processSink: ProcessSink[Out, Out2],
                   timeout: FiniteDuration): Source[Out2, Future[M]] = {
      require(timeout > Duration.Zero, s"timeout must be > 0, but was $timeout!")

      Source
        .setup { (mat, _) => // We need the `ActorMaterializer` to get its `system`!
          val parallelism = mat.system.settings.config.getInt("streamee.max-into-parallelism")
          source
            .map { out =>
              val (respondee2, out2) = Respondee.spawn[Out2](timeout)(mat)
              (out, respondee2, out2)
            }
            .alsoTo {
              Flow[(Out, Respondee[Out2], Promise[Out2])]
                .map { case (out, respondee2, _) => (out, respondee2) }
                .to(processSink)
            }
            .mapAsync(parallelism) { case (_, _, out2) => out2.future }
        }
    }
  }

  /**
    * Extension methods for `FlowWithContext`.
    */
  implicit final class FlowWithContextExt[In, CtxIn, Out, CtxOut, M](
      val flowWithContext: FlowWithContext[In, CtxIn, Out, CtxOut, M]
  ) extends AnyVal {

    /**
      * Ingest into the given [[ProcessSink]] and emit its response.
      *
      * @param processSink [[ProcessSink]] to emit into
      * @param timeout maximum duration for the running process to respond; must be positive!
      * @tparam Out2 response type of the given [[ProcessSink]]
      */
    def into[Out2](processSink: ProcessSink[Out, Out2],
                   timeout: FiniteDuration): FlowWithContext[In, CtxIn, Out2, CtxOut, Future[M]] = {
      require(timeout > Duration.Zero, s"timeout must be > 0, but was $timeout!")

      FlowWithContext.fromTuples(
        Flow
          .setup { (mat, _) => // We need the `ActorMaterializer` to get its `system`!
            val parallelism = mat.system.settings.config.getInt("streamee.max-into-parallelism")
            flowWithContext
              .map { out =>
                val (respondee2, out2) = Respondee.spawn[Out2](timeout)(mat)
                (out, respondee2, out2)
              }
              .via(Flow.apply.alsoTo {
                Flow[((Out, Respondee[Out2], Promise[Out2]), CtxOut)]
                  .map { case ((out, respondee2, _), _) => (out, respondee2) }
                  .to(processSink)
              })
              .mapAsync(parallelism) { case (_, _, out2) => out2.future }
              .asFlow
          }
      )
    }

    /**
      * Push the emitted element transformed by the given function `f` to the propagated context and
      * also transform the emitted element by the given function `g`.
      *
      * @param f transform the emitted element before pushing to the context
      * @param g transform the emitted element
      * @tparam A target type of the transformation of the element pushed to the context
      * @tparam B target type of the transformation of the element
      */
    def push[A, B](f: Out => A, g: Out => B): FlowWithContext[In, CtxIn, B, (A, CtxOut), Any] =
      flowWithContext.via(Flow.apply.map { case (out, ctxOut) => (g(out), (f(out), ctxOut)) })

    /**
      * Push the emitted element to the propagated context.
      */
    def push: FlowWithContext[In, CtxIn, Out, (Out, CtxOut), Any] =
      push(identity, identity)
  }

  /**
    * Extension methods for `FlowWithContext` with paired output context (see
    * [[FlowWithContextExt.push]]).
    */
  implicit final class FlowWithPairedContextOps[In, CtxIn, Out, CtxOut, A](
      val flowWithContext: FlowWithContext[In, CtxIn, Out, (A, CtxOut), Any]
  ) extends AnyVal {

    /**
      * Pop a formerly pushed and potentially transformed element from the propagated context and
      * pair it up with the emitted element.
      */
    def pop: FlowWithContext[In, CtxIn, (A, Out), CtxOut, Any] =
      flowWithContext.via(Flow.apply.map { case (out, (a, ctxOut)) => ((a, out), ctxOut) })
  }

  /**
    * Missing standard operators from `FlowOps` not yet defined on `FlowWithContext` (by Akka).
    */
  implicit final class FlowWithContextOpsAkka[In, CtxIn, Out, CtxOut](
      val flowWithContext: FlowWithContext[In, CtxIn, Out, CtxOut, Any]
  ) extends AnyVal {

    def delay(
        of: FiniteDuration,
        strategy: DelayOverflowStrategy = DelayOverflowStrategy.dropTail
    ): flowWithContext.Repr[Out, CtxOut] =
      flowWithContext.via(Flow.apply.delay(of, strategy))

    def throttle(elements: Int,
                 per: FiniteDuration,
                 maximumBurst: Int,
                 mode: ThrottleMode): flowWithContext.Repr[Out, CtxOut] =
      flowWithContext.via(Flow.apply.throttle(elements, per, maximumBurst, mode))
  }
}
