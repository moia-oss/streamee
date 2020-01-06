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

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ BroadcastHub, Flow, GraphDSL, Keep, MergeHub, Sink, Source }
import akka.NotUsed
import akka.stream.{ FlowShape, Materializer }
import scala.concurrent.duration.DurationInt

object Scratch {

  sealed trait Error
  final object Error {
    final case class OddNumber(n: Int)         extends Error
    final case class TooLargeNumber(n: Int)    extends Error
    final case class WayTooLargeNumber(n: Int) extends Error
  }

  implicit final class FlowExt[In, Out, E, Mat](val flow: Flow[In, Either[E, Out], Mat])
      extends AnyVal {
    def errorTo(errors: Sink[E, Any]): Flow[In, Out, Mat] =
      flow
        .alsoTo(
          Flow[Either[E, Out]]
            .collect { case Left(e) => e }
            .to(errors)
        )
        .collect { case Right(n) => n }
  }

  def tapErrors[In, Out, E](
      process: Sink[E, Any] => Flow[In, Either[E, Out], Any]
  )(implicit mat: Materializer): Flow[In, Either[E, Out], Any] = {
    val (errorTap, errors) =
      MergeHub
        .source[E]
        .toMat(BroadcastHub.sink[E])(Keep.both)
        .run()
    process(errorTap).merge(errors.map(Left.apply), eagerComplete = true)
  }

  def main(args: Array[String]): Unit =
    errorSink()

  private def errorSink() = {
    implicit val system: ActorSystem = ActorSystem()

    val process: Flow[Int, Either[Error, Int], Any] =
      tapErrors { errorTap =>
        Flow[Int]
          .map(n => if (n % 2 != 0) Left(Error.OddNumber(n)) else Right(n))
          .errorTo(errorTap)
          .map(n => if (n > 10) Left(Error.TooLargeNumber(n)) else Right(n))
          .errorTo(errorTap)
          .map(n => if (n > 20) Left(Error.WayTooLargeNumber(n)) else Right(n))
      }

    val done =
      Source(22.to(5).by(-1))
        .zipWith(Source.tick(0.millis, 250.millis, NotUsed)) { case (n, _) => n }
        .via(process)
        .runForeach(println)

    import system.dispatcher
    done.onComplete { result =>
      println(s"terminating ...: $result")
      system.terminate()
    }
    system.whenTerminated.onComplete { result =>
      println(s"terminated: $result")
    }
  }
}
