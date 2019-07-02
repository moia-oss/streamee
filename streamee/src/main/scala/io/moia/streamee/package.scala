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
import akka.actor.typed.scaladsl.adapter.UntypedActorSystemOps
import akka.stream.scaladsl.{ Flow, FlowWithContext, Sink, Source }
import akka.stream.DelayOverflowStrategy
import scala.concurrent.{ ExecutionContext, Promise }
import scala.concurrent.duration.FiniteDuration

package object streamee {

  /**
    * A domain logic process or process stage from an input to an output which transparently
    * propagates a [[Respondee]] for the top-level response. For a top-level process Out == Res. Can
    * be used locally or remotely.
    */
  type Process[-In, Out, Res] = FlowWithContext[In, Respondee[Res], Out, Respondee[Res], Any]

  /**
    * Convenient shortcut for `Sink[(Req, Respondee[Res]), Any]`.
    */
  type ProcessSink[Req, Res] = Sink[(Req, Respondee[Res]), Any]

  /**
    * Convenient shortcut for `ActorRef[Respondee.Response[A]]`.
    */
  type Respondee[A] = ActorRef[Respondee.Response[A]]

  // Needs to be more flexible than for Processes, because of pushing and popping which turns a
  // Process into more common FlowWithContext!

  //  /**
//    * Extension methods for [[Process]]es.
//    */
//  implicit final class ProcessOps[In, Out, Res](val process: Process[In, Out, Res]) extends AnyVal {
//
//    /**
//      * Emit into the given "intoable" `Sink` and continue with its response.
//      *
//      * @param sink    "intoable" sink from [[Process.runToIntoableSink]]
//      * @param timeout maximum duration for the sink to respond
//      */
//    def into[Out2](
//        sink: Sink[(Out, Respondee[Out2]), Any],
//        timeout: FiniteDuration
//    )(implicit ec: ExecutionContext, scheduler: Scheduler): Process[In, Out2, Res] =
//      FlowWithContext.fromTuples(
//        Flow
//          .setup { (mat, _) => // We need the `ActorMaterializer` to get its `system`!
//            val maxIntoParallelism =
//              mat.system.settings.config.getInt("streamee.max-into-parallelism")
//            process
//              .map { out =>
//                val promisedOut2 = Promise[Out2]()
//                val respondee2   = mat.system.spawnAnonymous(Respondee(promisedOut2, timeout))
//                (out, promisedOut2, respondee2)
//              }
//              .via(Flow.apply.alsoTo {
//                Flow[((Out, Promise[Out2], Respondee[Out2]), Respondee[Res])]
//                  .map { case ((out, _, respondee2), _) => (out, respondee2) }
//                  .to(sink)
//              })
//              .mapAsync(maxIntoParallelism) { case (_, promisedOut2, _) => promisedOut2.future }
//              .asFlow
//          }
//      )
//  }

  // TODO Maybe useful for plain Sources for subprocesses?

  /**
    * Extension methods for `Source`s.
    */
  implicit final class SourceExt[In, Out](val source: Source[Out, Any]) extends AnyVal {

    /**
      * Emit into the given "intoable" `Sink` and continue with its response.
      *
      * @param processSink    "intoable" sink from [[Process.runToIntoableSink]]
      * @param timeout maximum duration for the sink to respond
      */
    def into[Out2](
        processSink: ProcessSink[Out, Out2],
        timeout: FiniteDuration
    )(implicit ec: ExecutionContext, scheduler: Scheduler): Source[Out2, Any] =
      Source
        .setup { (mat, _) => // We need the `ActorMaterializer` to get its `system`!
          val maxIntoParallelism =
            mat.system.settings.config.getInt("streamee.max-into-parallelism")
          source
            .map { out =>
              val promisedOut2 = Promise[Out2]()
              val respondee2   = mat.system.spawnAnonymous(Respondee(promisedOut2, timeout))
              (out, promisedOut2, respondee2)
            }
            .alsoTo {
              Flow[(Out, Promise[Out2], Respondee[Out2])]
                .map { case (out, _, respondee2) => (out, respondee2) }
                .to(processSink)
            }
            .mapAsync(maxIntoParallelism) { case (_, promisedOut2, _) => promisedOut2.future }
        }
  }

  /**
    * Extension methods for `FlowWithContext`.
    */
  implicit final class FlowWithContextExt[In, CtxIn, Out, CtxOut](
      val flowWithContext: FlowWithContext[In, CtxIn, Out, CtxOut, Any]
  ) extends AnyVal {

    /**
      * Emit into the given "intoable" `Sink` and continue with its response.
      *
      * @param processSink    "intoable" sink from [[Process.runToIntoableSink]]
      * @param timeout maximum duration for the sink to respond
      */
    def into[Out2](processSink: ProcessSink[Out, Out2], timeout: FiniteDuration)(
        implicit ec: ExecutionContext,
        scheduler: Scheduler
    ): FlowWithContext[In, CtxIn, Out2, CtxOut, Any] =
      FlowWithContext.fromTuples(
        Flow
          .setup { (mat, _) => // We need the `ActorMaterializer` to get its `system`!
            val maxIntoParallelism =
              mat.system.settings.config.getInt("streamee.max-into-parallelism")
            flowWithContext
              .map { out =>
                val promisedOut2 = Promise[Out2]()
                val respondee2   = mat.system.spawnAnonymous(Respondee(promisedOut2, timeout))
                (out, promisedOut2, respondee2)
              }
              .via(Flow.apply.alsoTo {
                Flow[((Out, Promise[Out2], Respondee[Out2]), CtxOut)]
                  .map { case ((out, _, respondee2), _) => (out, respondee2) }
                  .to(processSink)
              })
              .mapAsync(maxIntoParallelism) { case (_, promisedOut2, _) => promisedOut2.future }
              .asFlow
          }
      )

    // TODO Doc comments!
    def push[A, B](f: Out => A, g: Out => B): FlowWithContext[In, CtxIn, B, (A, CtxOut), Any] =
      flowWithContext.via(Flow.apply.map { case (out, ctxOut) => (g(out), (f(out), ctxOut)) })

    // TODO Doc comments!
    def push: FlowWithContext[In, CtxIn, Out, (Out, CtxOut), Any] =
      push(identity, identity)
  }

  /**
    * Extension methods for `FlowWithContext` with paired output context (see
    * [[FlowWithContextExt.push]]).
    */
  implicit final class FlowWithContextExt2[In, CtxIn, Out, CtxOut, A](
      val flowWithContext: FlowWithContext[In, CtxIn, Out, (A, CtxOut), Any]
  ) extends AnyVal {

    // TODO Doc comments!
    def pop: FlowWithContext[In, CtxIn, (A, Out), CtxOut, Any] =
      flowWithContext.via(Flow.apply.map { case (out, (a, ctxOut)) => ((a, out), ctxOut) })
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
