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

import akka.stream.scaladsl.{ Flow, FlowWithContext, Sink, Source, SourceWithContext }
import akka.NotUsed
import org.scalacheck.Gen
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import scala.concurrent.duration.DurationInt

final class StreameeTests
    extends AsyncWordSpec
    with AkkaSuite
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  "Calling into on a Source" should {
    "throw an IllegalArgumentException for timeout <= 0" in {
      forAll(TestData.nonPosDuration) { timeout =>
        an[IllegalArgumentException] shouldBe thrownBy {
          Source.single("abc").into(Sink.ignore, timeout, 42)
        }
      }
    }

    "throw an IllegalArgumentException for parallelism <= 0" in {
      forAll(Gen.choose(Int.MinValue, 0)) { parallelism =>
        an[IllegalArgumentException] shouldBe thrownBy {
          Source.single("abc").into(Sink.ignore, 1.second, parallelism)
        }
      }
    }

    "result in a TimeoutException if the ProcessSink does not respond in time" in {
      val timeout = 100.milliseconds
      Source
        .single("abc")
        .into(Sink.ignore, timeout, 42)
        .runWith(Sink.head)
        .failed
        .map { case ResponseTimeoutException(t) => t shouldBe timeout }
    }

    "ingest into the process sink and emit its response" in {
      val processSink =
        Sink.foreach[(String, Respondee[String])] {
          case (s, r) => r ! Respondee.Response(s.toUpperCase)
        }
      Source
        .single("abc")
        .into(processSink, 1.second, 42)
        .runWith(Sink.head)
        .map(_ shouldBe "ABC")
    }
  }

  "Calling into on a Flow" should {
    "throw an IllegalArgumentException for timeout <= 0" in {
      forAll(TestData.nonPosDuration) { timeout =>
        an[IllegalArgumentException] shouldBe thrownBy {
          Flow[String].into(Sink.ignore, timeout, 42)
        }
      }
    }

    "throw an IllegalArgumentException for parallelism <= 0" in {
      forAll(Gen.choose(Int.MinValue, 0)) { parallelism =>
        an[IllegalArgumentException] shouldBe thrownBy {
          Flow[String].into(Sink.ignore, 1.second, parallelism)
        }
      }
    }

    "result in a TimeoutException if the ProcessSink does not respond in time" in {
      val timeout = 100.milliseconds
      val flow    = Flow[String].into(Sink.ignore, timeout, 42)
      Source
        .single("abc")
        .via(flow)
        .runWith(Sink.head)
        .failed
        .map { case ResponseTimeoutException(t) => t shouldBe timeout }
    }

    "ingest into the process sink and emit its response" in {
      val processSink =
        Sink.foreach[(String, Respondee[String])] {
          case (s, r) => r ! Respondee.Response(s.toUpperCase)
        }
      val flow = Flow[String].into(processSink, 1.second, 42)
      Source
        .single("abc")
        .via(flow)
        .runWith(Sink.head)
        .map(_ shouldBe "ABC")
    }
  }

  "Calling into on a FlowWithContext" should {
    "throw an IllegalArgumentException for timeout <= 0" in {
      forAll(TestData.nonPosDuration) { timeout =>
        an[IllegalArgumentException] shouldBe thrownBy {
          FlowWithContext[String, Respondee[String]].into(Sink.ignore, timeout, 42)
        }
      }
    }

    "throw an IllegalArgumentException for parallelism <= 0" in {
      forAll(Gen.choose(Int.MinValue, 0)) { parallelism =>
        an[IllegalArgumentException] shouldBe thrownBy {
          FlowWithContext[String, Respondee[String]].into(Sink.ignore, 1.second, parallelism)
        }
      }
    }

    "result in a TimeoutException if the ProcessSink does not respond in time" in {
      val timeout        = 100.milliseconds
      val (respondee, _) = Respondee.spawn[String](timeout)
      SourceWithContext
        .fromTuples(Source.single(("abc", respondee)))
        .via(FlowWithContext[String, Respondee[String]].into(Sink.ignore, timeout, 42))
        .runWith(Sink.head)
        .failed
        .map { case ResponseTimeoutException(t) => t shouldBe timeout }
    }

    "ingest into the process sink and emit its response" in {
      val (respondee, _) = Respondee.spawn[String](1.second)
      val processSink =
        Sink.foreach[(String, Respondee[String])] {
          case (s, r) => r ! Respondee.Response(s.toUpperCase)
        }
      SourceWithContext
        .fromTuples(Source.single(("abc", respondee)))
        .via(FlowWithContext[String, Respondee[String]].into(processSink, 1.second, 42))
        .runWith(Sink.head)
        .map(_._1 shouldBe "ABC")
    }
  }

  "Calling push and pop" should {
    "first push each elements to the propagated context and then pop it" in {
      val flow =
        FlowWithContext[String, NotUsed]
          .map(_.toUpperCase)
          .push
          .map(_.length)
          .pop

      Source
        .single(("abc", NotUsed))
        .via(flow)
        .runWith(Sink.head)
        .map {
          case ((s, n), _) =>
            s shouldBe "ABC"
            n shouldBe 3
        }
    }

    "first push and transform each elements to the propagated context and then pop and transform it" in {
      val flow =
        FlowWithContext[String, NotUsed]
          .push(_.toUpperCase, _ * 2)
          .map(_.length)
          .pop

      Source
        .single(("abc", NotUsed))
        .via(flow)
        .runWith(Sink.head)
        .map {
          case ((s, n), _) =>
            s shouldBe "ABC"
            n shouldBe 6
        }
    }
  }

  "Calling errorTo" should {
    "tap errors into the given Sink" in {
      val (errors, errorTap) = Sink.seq[(String, NotUsed)].preMaterialize()
      val flow               = FlowWithContext[Either[String, Int], NotUsed].errorTo(errorTap)
      SourceWithContext
        .fromTuples(Source(List(Right(1), Left("a"), Right(2), Left("b")).map((_, NotUsed))))
        .via(flow)
        .runWith(Sink.seq)
        .zip(errors)
        .map {
          case (ns, errors) =>
            ns.map(_._1) shouldBe List(1, 2)
            errors.map(_._1) shouldBe List("a", "b")
        }
    }
  }

  "Calling asFrontProcessor" should {
    "convert an IntoableSink into a FrontProcessor" in {
      val process           = Process[String, Int].map(_.length)
      val intoableProcessor = IntoableProcessor(process, "name")
      val frontProcessor    = intoableProcessor.sink.asFrontProcessor(1.second, 42, "name")
      frontProcessor
        .offer("abc")
        .map(_ shouldBe 3)
    }
  }

  "Calling zipWithIn" should {
    "wrap the given step in one emitting its input together with its output" in {
      def length[Ctx]                                = Step[String, Ctx].map(_.length)
      val step: Step[String, (String, Int), NotUsed] = zipWithIn(length)
      val test                                       = "test"
      SourceWithContext
        .fromTuples(Source.single((test, NotUsed)))
        .via(step)
        .runWith(Sink.head)
        .map(_ shouldBe ((test, test.length), NotUsed))
    }
  }

  "Calling tapErrors" should {
    "create a FlowWithContext by providing an error Sink" in {
      val flow: FlowWithContext[Either[String, Int], NotUsed, Either[String, Int], NotUsed, Any] =
        tapErrors { errorTap =>
          FlowWithContext[Either[String, Int], NotUsed].errorTo(errorTap)
        }
      val elements =
        1.to(100)
          .flatMap(n => List(Right(n), Left(n.toString)))
          .map((_, NotUsed))
      SourceWithContext
        .fromTuples(Source(elements))
        .via(flow)
        .runWith(Sink.seq)
        .map(_ should contain theSameElementsAs elements)
    }

    "allow the created FlowWithContext to be run twice sequentially" in {
      val flow: FlowWithContext[Either[String, Int], NotUsed, Either[String, Int], NotUsed, Any] =
        tapErrors { errorTap =>
          FlowWithContext[Either[String, Int], NotUsed].errorTo(errorTap)
        }
      val elements =
        1.to(100)
          .flatMap(n => List(Right(n), Left(n.toString)))
          .map((_, NotUsed))
      val source =
        SourceWithContext
          .fromTuples(Source(elements))
          .via(flow)
      val results =
        for {
          result1 <- source.runWith(Sink.seq)
          result2 <- source.runWith(Sink.seq)
        } yield (result1, result2)
      results.map {
        case (result1, result2) =>
          result1 should contain theSameElementsAs elements
          result2 should contain theSameElementsAs elements
      }
    }

    "allow the created FlowWithContext to be run twice concurrently" in {
      val flow: FlowWithContext[Either[String, Int], NotUsed, Either[String, Int], NotUsed, Any] =
        tapErrors { errorTap =>
          FlowWithContext[Either[String, Int], NotUsed].errorTo(errorTap)
        }
      val elements =
        1.to(100)
          .flatMap(n => List(Right(n), Left(n.toString)))
          .map((_, NotUsed))
      val source =
        SourceWithContext
          .fromTuples(Source(elements))
          .via(flow)
      val result1 = source.runWith(Sink.seq)
      val result2 = source.runWith(Sink.seq)
      val results = result1.zip(result2)
      results.map {
        case (result1, result2) =>
          result1 should contain theSameElementsAs elements
          result2 should contain theSameElementsAs elements
      }
    }
  }
}
