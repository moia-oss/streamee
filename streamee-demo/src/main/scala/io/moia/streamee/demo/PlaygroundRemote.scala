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

import akka.actor.ActorSystem
import akka.event.{ Logging, LoggingAdapter }
import akka.stream.{ ActorMaterializer, DelayOverflowStrategy, Materializer, ThrottleMode }
import akka.stream.scaladsl.{
  Flow,
  Keep,
  MergeHub,
  RestartSink,
  Sink,
  Source,
  StreamRefs
}
import io.moia.streamee.ExpiringPromise
import scala.concurrent.{ Await, Future, Promise }
import scala.concurrent.duration.DurationInt

object PlaygroundRemote {

  // ###############################################################################################
  // RUNNER
  // ###############################################################################################

  def main(args: Array[String]): Unit = {
    implicit val system              = ActorSystem()
    implicit val mat                 = ActorMaterializer()
    implicit val scheduler           = system.scheduler
    implicit val log: LoggingAdapter = Logging(system, getClass.getName)

    import system.dispatcher

    val intoableFlow =
      Flow[(Int, Promise[String])]
        .delay(1.second, DelayOverflowStrategy.backpressure)
        .throttle(1, 1.second, 10, ThrottleMode.shaping)
        .map { case (n, p) => ("x" * n, p) }

    def runIntoableFlow[A, B](
        intoableFlow: Flow[(A, Promise[B]), (B, Promise[B]), Any],
        bufferSize: Int
    )(implicit mat: Materializer): Sink[(A, Promise[B]), Any] = {
      val (mergeHubSink, intoableDone) =
        MergeHub
          .source[(A, Promise[B])](bufferSize)
          .via(intoableFlow)
          .toMat(Sink.foreach { case (b, p) => p.trySuccess(b) })(Keep.both)
          .run()
      intoableDone.onComplete(println)
      mergeHubSink
    }

    val intoableSink = runIntoableFlow(intoableFlow, 1)

    def getIntoableSinkRef[A, B](intoableSink: Sink[(A, Promise[B]), Any]) = {
      println("Getting SinkRef")
      val sinkRef = Await.result(StreamRefs.sinkRef().to(intoableSink).run(), 1.second)
      Flow[(A, Promise[B])]
        .take(7)
        .to(sinkRef) // Simulating completion/failure of the SinkRef after 7 elements
    }

    val done =
      Source(1.to(100))
      // into-start
        .map(a => (a, ExpiringPromise[String](10.seconds, s"n = $a")))
        .alsoTo(RestartSink.withBackoff(2.seconds, 4.seconds, 0) { () =>
          getIntoableSinkRef(intoableSink)
        })
        .mapAsync(1)(_._2.future)
        // into-end
        //.delay(2.seconds, DelayOverflowStrategy.backpressure)
        .toMat(Sink.foreach { s =>
          println(s"client-out: ${s.length}")
        })(Keep.right)
        .run()

    Future.sequence(List(done)).onComplete { result =>
      println(result)
      system.terminate()
    }
  }
}
