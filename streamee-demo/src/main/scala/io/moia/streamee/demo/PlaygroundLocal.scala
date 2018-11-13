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
package demo

import akka.actor.ActorSystem
import akka.stream.{ ActorMaterializer, DelayOverflowStrategy, KillSwitches, ThrottleMode }
import akka.stream.scaladsl.{ Flow, Keep, Sink, Source }
import io.moia.streamee.intoable._
import scala.concurrent.{ Future, Promise }
import scala.concurrent.duration.DurationInt

object PlaygroundLocal {

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

    val (intoableSink, _) = runIntoableProcess(intoableFlow, 1)

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
