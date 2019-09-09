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

package io.moia.streamee4
package processor

import akka.stream.scaladsl.Flow
import akka.stream.{ DelayOverflowStrategy, OverflowStrategy }
import akka.testkit.TestDuration
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import utest._

object PermanentProcessorTests extends ActorTestSuite {
  import testKit._

  private val plusOne = Flow[Int].map(_ + 1)

  override def tests: Tests =
    Tests {
      'applyIllegalTimeout - {
        intercept[IllegalArgumentException] {
          Processor.permanent(plusOne, 0.seconds, "processor", 0)(identity, _ - 1)
        }
      }

      'applyIllegalBufferSize - {
        intercept[IllegalArgumentException] {
          Processor.permanent(plusOne, 0.seconds, "processor", -1)(identity, _ - 1)
        }
      }

      'inTime - {
        val timeout   = 100.milliseconds.dilated
        val processor = Processor.permanent(plusOne, timeout, "processor", 0)(identity, _ - 1)

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
        val processor = Processor.permanent(process, timeout, "processor", 0)(identity, _ - 1)

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

      'reorder - {
        val timeout   = 100.milliseconds.dilated
        val process   = plusOne.grouped(2).mapConcat { case Seq(n1, n2) => List(n2, n1) }
        val processor = Processor.permanent(process, timeout, "processor", 0)(identity, _ - 1)

        Future
          .sequence(List(42, 43, 44, 45).map(processor.process))
          .map { responses =>
            assert {
              responses == List(43, 44, 45, 46)
            }
          }
      }

      'filter - {
        val timeout   = 100.milliseconds.dilated
        val process   = plusOne.filter(_ % 2 != 0)
        val processor = Processor.permanent(process, timeout, "processor", 0)(identity, _ - 1)

        val pe43 = PromiseExpired(timeout, "from processor processor for request 43")
        val pe45 = PromiseExpired(timeout, "from processor processor for request 45")

        Future
          .sequence(
            List(
              processor.process(42),
              processor.process(43).failed,
              processor.process(44),
              processor.process(45).failed
            )
          )
          .map { responses =>
            assert {
              responses == List(43, pe43, 45, pe45)
            }
          }
      }

      'resume - {
        val timeout   = 100.milliseconds.dilated
        val process   = plusOne.map(n => if (n % 2 == 0) throw new Exception("boom") else n)
        val processor = Processor.permanent(process, timeout, "processor", 0)(identity, _ - 1)

        val pe43 = PromiseExpired(timeout, "from processor processor for request 43")
        val pe45 = PromiseExpired(timeout, "from processor processor for request 45")

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
              responses == List(43, pe43, 45, pe45)
            }
          }
      }

      'processInFlightOnShutdown - {
        val timeout   = 1000.milliseconds.dilated
        val process   = plusOne.delay(100.milliseconds.dilated, DelayOverflowStrategy.backpressure)
        val processor = Processor.permanent(process, timeout, "processor", 0)(identity, _ - 1)

        val responses = Future.sequence(List(42, 43, 44, 45).map(processor.process))
        for {
          _  <- processor.shutdown()
          rs <- responses
        } yield assert(rs == List(43, 44, 45, 46))
      }

      'noLongerEnqueueOnShutdown - {
        val timeout   = 100.milliseconds.dilated
        val process   = plusOne.delay(100.milliseconds.dilated, DelayOverflowStrategy.backpressure)
        val processor = Processor.permanent(process, timeout, "processor", 0)(identity, _ - 1)

        processor.shutdown()
        Future
          .sequence(List(42, 43, 44, 45).map(processor.process).map(_.failed))
          .map { responses =>
            assert(responses.map(_ => "failure") == List.fill(4)("failure"))
          }
      }
    }
}
