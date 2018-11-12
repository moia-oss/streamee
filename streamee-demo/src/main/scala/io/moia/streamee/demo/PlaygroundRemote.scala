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
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.ActorRef
import akka.stream.{ ActorMaterializer, DelayOverflowStrategy, ThrottleMode }
import akka.stream.scaladsl.{ Flow, Keep, RestartSink, Sink, Source, StreamRefs }
import io.moia.streamee.intoable._
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration.DurationInt

object PlaygroundRemote {

  // ###############################################################################################
  // RUNNER
  // ###############################################################################################

  def main(args: Array[String]): Unit = {
    implicit val system    = ActorSystem()
    implicit val mat       = ActorMaterializer()
    implicit val scheduler = system.scheduler

    import system.dispatcher

    val intoableFlow =
      Flow[(Int, ActorRef[Respondee.Command[String]])]
        .delay(1.second, DelayOverflowStrategy.backpressure)
        .throttle(1, 1.second, 10, ThrottleMode.shaping)
        .map { case (n, p) => ("x" * n, p) }

    val (intoableSink, _) = runRemotelyIntoableFlow(intoableFlow, 1)

    def getIntoableSinkRef[A, B](intoableSink: Sink[(A, ActorRef[Respondee.Command[B]]), Any]) = {
      println("Getting SinkRef")
      val sinkRef = Await.result(StreamRefs.sinkRef().to(intoableSink).run(), 1.second)
      Flow[(A, ActorRef[Respondee.Command[B]])]
        .take(7)
        .to(sinkRef) // Simulating completion/failure of the SinkRef after 7 elements
    }

    val restartedIntoableSinkRef =
      RestartSink.withBackoff(2.seconds, 4.seconds, 0) { () =>
        getIntoableSinkRef(intoableSink)
      }

    implicit val respondeeFactory: ActorRef[RespondeeFactory.Command[String]] =
      system.spawnAnonymous(RespondeeFactory[String]())

    val done =
      Source(1.to(100))
        .intoRemote(restartedIntoableSinkRef, 10.seconds, 1)
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
