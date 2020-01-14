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
package either

import akka.stream.scaladsl.{ FlowWithContext, Sink, Source, SourceWithContext }
import akka.NotUsed
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

final class EitherTests
    extends AsyncWordSpec
    with AkkaSuite
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

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
