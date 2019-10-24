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

import akka.actor.{ ActorSystem, CoordinatedShutdown }
import akka.pattern.{ after => akkaAfter }
import akka.stream.{ ActorAttributes, Materializer, StreamRefAttributes, Supervision, ThrottleMode }
import akka.stream.scaladsl.{ Sink, Source }
import org.scalacheck.Gen
import org.scalatest.{ AsyncWordSpec, Matchers }
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import scala.concurrent.duration.DurationInt
import scala.concurrent.Future

final class IntoableProcessorTests
    extends AsyncWordSpec
    with AkkaSuite
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  "Creating an IntoableProcessor" should {
    "throw an IllegalArgumentException for bufferSize <= 0" in {
      forAll(Gen.choose(Int.MinValue, 0)) { bufferSize =>
        an[IllegalArgumentException] shouldBe thrownBy {
          IntoableProcessor(Process[Int, Int](), "name", bufferSize)
        }
      }
    }
  }

  "Calling shutdown" should {
    "fail after the given timeout" in {
      val timeout   = 100.milliseconds
      val process   = Process[String, String]().delay(1.second)
      val processor = FrontProcessor(process, timeout, "name")
      processor
        .offer("abc")
        .failed
        .map(_ shouldBe ResponseTimeoutException(timeout))
    }

    "complete whenDone" in {
      val processor = IntoableProcessor(Process[Int, Int](), "name")
      val done      = processor.whenDone
      processor.shutdown()
      done.map(_ => succeed)
    }
  }

  "Using an IntoableProcessor locally" should {
    "eventually emit into the outer stream" in {
      val process   = Process[String, Int]().map(_.length)
      val processor = IntoableProcessor(process, "name")
      Source
        .single("abc")
        .into(processor.sink, 1.second, 42)
        .runWith(Sink.head)
        .map(_ shouldBe 3)
    }

    "fail after the given timeout" in {
      val timeout   = 100.milliseconds
      val process   = Process[String, String]().delay(timeout * 3)
      val processor = IntoableProcessor(process, "name")
      Source
        .single("abc")
        .into(processor.sink, timeout, 42)
        .runWith(Sink.headOption)
        .failed
        .map(_ shouldBe ResponseTimeoutException(timeout))
    }

    "resume on failure" in {
      val process   = Process[Int, Int]().map(42 / _)
      val processor = IntoableProcessor(process, "name")
      Source(List(1, 0, 2, 0, 3))
        .into(processor.sink, 1.second, 42)
        .addAttributes(ActorAttributes.supervisionStrategy(resumeOnTimeout))
        .runWith(Sink.seq)
        .map(_ shouldBe List(42, 21, 14))
    }

    "at most drop as many requests as the bufferSize on shutdown" in {
      val process   = Process[Int, Int]().throttle(1, 100.milliseconds, 0, ThrottleMode.Shaping)
      val processor = IntoableProcessor(process, "name", 2)
      Source(1.to(10))
        .map { n =>
          if (n == 7) processor.shutdown()
          n
        }
        .into(processor.sink, 1.seconds, 42)
        .addAttributes(ActorAttributes.supervisionStrategy(resumeOnTimeout))
        .runWith(Sink.seq)
        .map(_.max should be >= 4) // 7 - 2, 2 from IntoableProcessor, 1 from delay btw shutdown and into
    }
  }

  "Using an IntoableProcessor remotely" should {
    "eventually emit into the outer stream" in {
      val process   = Process[String, Int]().map(_.length)
      val processor = IntoableProcessor(process, "name")
      Source
        .single("abc")
        .into(processor.sinkRef(), 1.second, 42)
        .runWith(Sink.head)
        .map(_ shouldBe 3)
    }

    "fail after the given timeout" in {
      val timeout   = 100.milliseconds
      val process   = Process[String, String]().delay(timeout * 3)
      val processor = IntoableProcessor(process, "name")
      Source
        .single("abc")
        .into(processor.sinkRef(), timeout, 42)
        .runWith(Sink.headOption)
        .failed
        .map(_ shouldBe ResponseTimeoutException(timeout))
    }

    "resume on failure" in {
      val process   = Process[Int, Int]().map(42 / _)
      val processor = IntoableProcessor(process, "name")
      Source(List(1, 0, 2, 0, 3))
        .into(processor.sinkRef(), 1.second, 42)
        .addAttributes(ActorAttributes.supervisionStrategy(resumeOnTimeout))
        .runWith(Sink.seq)
        .map(_ shouldBe List(42, 21, 14))
    }

    "at most drop as many requests as the bufferSize on shutdown" in {
      val process   = Process[Int, Int]().throttle(1, 100.milliseconds, 0, ThrottleMode.Shaping)
      val processor = IntoableProcessor(process, "name", 2)
      Source(1.to(10))
        .map { n =>
          if (n == 7) processor.shutdown()
          n
        }
        .into(processor.sinkRef(StreamRefAttributes.bufferCapacity(1)), 1.seconds, 42)
        .addAttributes(ActorAttributes.supervisionStrategy(resumeOnTimeout))
        .recover { case _ => Int.MinValue }
        .runWith(Sink.seq)
        .map(_.max should be >= 3) // 7 - 2 - 1 - 1, 2 from IntoableProcessor, 1 from SinkRef buffering, 1 from delay btw shutdown and into
    }
  }

  "CoordinatedShutdown" should {
    "shutdown the processor" in {
      val testSystem = ActorSystem()
      val testMat    = Materializer(testSystem)
      val processor  = IntoableProcessor(Process[Int, Int](), "name")(testMat)
      val late       = akkaAfter(5.second, scheduler)(Future.failed(new Exception("Late!")))
      val doneOrLate = Future.firstCompletedOf(List(processor.whenDone, late))
      CoordinatedShutdown(testSystem).run(CoordinatedShutdown.UnknownReason)
      doneOrLate.map(_ => succeed)
    }
  }

  private def resumeOnTimeout(t: Throwable) =
    t match {
      case _: ResponseTimeoutException => Supervision.Resume
      case _                           => Supervision.Stop
    }
}
