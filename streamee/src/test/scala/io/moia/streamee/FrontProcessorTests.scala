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
import akka.stream.Materializer
import org.scalacheck.Gen
import org.scalatest.{ AsyncWordSpec, Matchers }
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import scala.concurrent.duration.DurationInt
import scala.concurrent.Future

final class FrontProcessorTests
    extends AsyncWordSpec
    with AkkaSuite
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  "Creating a FrontProcessor" should {
    "throw an IllegalArgumentException for timeout <= 0" in {
      forAll(TestData.nonPosDuration) { timeout =>
        an[IllegalArgumentException] shouldBe thrownBy {
          FrontProcessor(Process[Int, Int](), timeout, "name")
        }
      }
    }

    "throw an IllegalArgumentException for bufferSize <= 0" in {
      forAll(Gen.choose(Int.MinValue, 0)) { bufferSize =>
        an[IllegalArgumentException] shouldBe thrownBy {
          FrontProcessor(Process[Int, Int](), 1.second, "name", bufferSize)
        }
      }
    }
  }

  "Calling offer" should {
    "eventually succeed" in {
      val process   = Process[String, Int]().map(_.length)
      val processor = FrontProcessor(process, 1.second, "name")
      processor
        .offer("abc")
        .map(_ shouldBe 3)
    }

    "fail after the given timeout" in {
      val timeout   = 100.milliseconds
      val process   = Process[String, String]().delay(1.second)
      val processor = FrontProcessor(process, timeout, "name")
      processor
        .offer("abc")
        .failed
        .map(_ shouldBe ResponseTimeoutException(timeout))
    }

    "resume on failure" in {
      val process   = Process[(Int, Int), Int]().map { case (n, m) => n / m }
      val processor = FrontProcessor(process, 1.second, "name")
      processor
        .offer((4, 0))
        .failed
        .map(_.getClass shouldBe classOf[ArithmeticException])
      processor
        .offer((4, 2))
        .map(_ shouldBe 2)
    }

    "process already offered requests on shutdown" in {
      val process   = Process[String, String]().delay(100.milliseconds)
      val processor = FrontProcessor(process, 1.second, "name")
      val response1 = processor.offer("abc")
      processor.shutdown()
      val response2 = processor.offer("def")
      response1.zip(response2.failed).map {
        case (s, e) =>
          s shouldBe "abc"
          e shouldBe FrontProcessor.ProcessorUnavailable("name")
      }
    }
  }

  "Calling shutdown" should {
    "complete whenDone" in {
      val processor = FrontProcessor(Process[Int, Int](), 1.second, "name")
      val done      = processor.whenDone
      processor.shutdown()
      for {
        _ <- done
        _ <- Future(processor.shutdown()) // Verify that shutdown is idempotent
      } yield succeed
    }
  }

  "CoordinatedShutdown" should {
    "shutdown the processor" in {
      val testSystem = ActorSystem()
      val testMat    = Materializer(testSystem)
      val processor =
        FrontProcessor(Process[Int, Int](), 1.second, "name")(
          testMat,
          testSystem.dispatcher
        )
      val late       = akkaAfter(5.seconds, scheduler)(Future.failed(new Exception("Late!")))
      val doneOrLate = Future.firstCompletedOf(List(processor.whenDone, late))
      CoordinatedShutdown(testSystem).run(CoordinatedShutdown.UnknownReason)
      doneOrLate.map(_ => succeed)
    }
  }
}
