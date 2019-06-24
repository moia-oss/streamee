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
package processor

import akka.stream.scaladsl.Flow
import akka.stream.{ DelayOverflowStrategy, OverflowStrategy }
import akka.testkit.TestDuration
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import utest._

object PerRequestProcessorTests extends ActorTestSuite {
  import testKit._

  private val plusOne = Flow[Int].map(_ + 1)

  override def tests: Tests =
    Tests {
      'applyIllegalTimeout - {
        intercept[IllegalArgumentException] {
          Handler.perRequest(plusOne, 0.seconds, "processor")
        }
      }

      'inTime - {
        val processor = Handler.perRequest(plusOne, 100.milliseconds.dilated, "processor")

        Future
          .sequence(List(42, 43, 44, 45).map(processor.process))
          .map { responses =>
            assert {
              responses == List(43, 44, 45, 46)
            }
          }
      }

      'inTimeHandler - {
        val processor =
          Handler.perRequest((n: Int) => Future.successful(n + 1),
                               100.milliseconds.dilated,
                               "processor")

        Future
          .sequence(List(42, 43, 44, 45).map(processor.process))
          .map { responses =>
            assert {
              responses == List(43, 44, 45, 46)
            }
          }
      }

      'notInTime - {
        val timeout   = 100.milliseconds.dilated
        val process   = plusOne.delay(1.second.dilated, OverflowStrategy.backpressure)
        val processor = Handler.perRequest(process, timeout, "processor")

        val pe42 = PromiseExpired(timeout, "from processor processor for request 42")
        val pe43 = PromiseExpired(timeout, "from processor processor for request 43")
        val pe44 = PromiseExpired(timeout, "from processor processor for request 44")
        val pe45 = PromiseExpired(timeout, "from processor processor for request 45")

        Future
          .sequence(List(42, 43, 44, 45).map(processor.process).map(_.failed))
          .map { responses =>
            assert {
              responses == List(pe42, pe43, pe44, pe45)
            }
          }
      }

      'resume - {
        val boom      = new Exception("boom")
        val process   = plusOne.map(n => if (n % 2 == 0) throw boom else n)
        val processor = Handler.perRequest(process, 100.milliseconds.dilated, "processor")

        Future
          .sequence {
            List(
              processor.process(42),
              processor.process(43).failed,
              processor.process(44),
              processor.process(45).failed
            )
          }
          .map { responses =>
            assert {
              responses == List(43, boom, 45, boom)
            }
          }
      }

      'processInFlightOnShutdown - {
        val process   = plusOne.delay(100.milliseconds.dilated, DelayOverflowStrategy.backpressure)
        val processor = Handler.perRequest(process, 1000.milliseconds.dilated, "processor")

        val responses = Future.sequence(List(42, 43, 44, 45).map(processor.process))
        for {
          _  <- processor.shutdown()
          rs <- responses
        } yield assert(rs == List(43, 44, 45, 46))
      }

      'noLongerEnqueueOnShutdown - {
        val process   = plusOne.delay(100.milliseconds.dilated, DelayOverflowStrategy.backpressure)
        val processor = Handler.perRequest(process, 1000.milliseconds.dilated, "processor")

        processor.shutdown()
        Future
          .sequence(List(42, 43, 44, 45).map(processor.process).map(_.failed))
          .map { responses =>
            assert(responses.map(_ => "failure") == List.fill(4)("failure"))
          }
      }
    }
}
