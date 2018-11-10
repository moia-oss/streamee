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

package io.moia.streamee.demo

import akka.actor.{ ActorSystem, Scheduler }
import akka.stream.{
  ActorMaterializer,
  DelayOverflowStrategy,
  KillSwitches,
  Materializer,
  ThrottleMode
}
import akka.stream.scaladsl.{ Flow, FlowOps, Keep, MergeHub, Sink, Source }
import io.moia.streamee.ExpiringPromise
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration.{ DurationInt, FiniteDuration }

object PlaygroundLocal {

  // ###############################################################################################
  // Local `into`
  // ###############################################################################################

  implicit final class SourceOps[A, M](val source: Source[A, M]) extends AnyVal {
    def into[B](
        intoableSink: Sink[(A, Promise[B]), Any],
        responseTimeout: FiniteDuration,
        parallelism: Int
    )(implicit ec: ExecutionContext, scheduler: Scheduler): Source[B, M] =
      intoImpl(source, intoableSink, responseTimeout, parallelism)
  }

  implicit final class FlowExt[A, B, M](val flow: Flow[A, B, M]) extends AnyVal {
    def into[C](
        intoableSink: Sink[(B, Promise[C]), Any],
        responseTimeout: FiniteDuration,
        parallelism: Int
    )(implicit ec: ExecutionContext, scheduler: Scheduler): Flow[A, C, M] =
      intoImpl(flow, intoableSink, responseTimeout, parallelism)
  }

  private def intoImpl[A, B, M](
      flowOps: FlowOps[A, M],
      intoableSink: Sink[(A, Promise[B]), Any],
      responseTimeout: FiniteDuration,
      parallelism: Int
  )(implicit ec: ExecutionContext, scheduler: Scheduler): flowOps.Repr[B] =
    flowOps
      .map(a => (a, ExpiringPromise[B](responseTimeout)))
      .alsoTo(intoableSink)
      .mapAsync(parallelism)(_._2.future)

  // ###############################################################################################
  // Local `runIntoableFlow`
  // ###############################################################################################

  def runIntoableFlow[A, B](intoableFlow: Flow[(A, Promise[B]), (B, Promise[B]), Any],
                            bufferSize: Int)(
      implicit mat: Materializer
  ): Sink[(A, Promise[B]), Any] =
    MergeHub
      .source[(A, Promise[B])](bufferSize)
      .via(intoableFlow)
      .to(Sink.foreach { case (b, p) => p.trySuccess(b) })
      .run()

  // ###############################################################################################
  // RUNNER
  // ###############################################################################################

  def main(args: Array[String]): Unit = {
    implicit val system    = ActorSystem()
    implicit val mat       = ActorMaterializer()
    implicit val scheduler = system.scheduler

    import system.dispatcher

    val intoableFlow =
      Flow[(Int, Promise[String])]
        .delay(1.second, DelayOverflowStrategy.backpressure)
        .throttle(1, 1.second, 10, ThrottleMode.shaping)
        .map { case (n, p) => ("x" * n, p) }

    val intoableSink =
      runIntoableFlow(intoableFlow, 1)

    val (switch, done) =
      Source(1.to(100))
        .viaMat(KillSwitches.single)(Keep.right)
        .into(intoableSink, 30.seconds, 42)
        .delay(2.seconds, DelayOverflowStrategy.backpressure)
        .toMat(Sink.foreach { s =>
          println(s"client-out: ${s.length}")
        })(Keep.both)
        .run()

    akka.pattern.after(5.seconds, scheduler)(Future.successful {
      println("## Shutting down!")
      switch.shutdown()
    })

    Future.sequence(List(done)).onComplete { result =>
      println(result)
      system.terminate()
    }
  }
}
