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

import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Flow
import akka.testkit.TestDuration
import scala.collection.immutable.Seq
import scala.concurrent.{ Future, Promise }
import scala.concurrent.duration.DurationInt
import utest._

object ProcessorTests extends ActorTestSuite {
  import testKit._

  private val plusOne = IntoableProcessor[Int, Int]().map(_ + 1)

  override def tests: Tests =
    Tests {
      'handlerIllegalParallelism - {
        intercept[IllegalArgumentException] {
          Handler((n: Int) => Future.successful(n + 1), 1.second, "processor", 1, 0)
        }
      }

      'applyIllegalTimeout - {
        intercept[IllegalArgumentException] {
          Handler(plusOne, 0.seconds, "processor", 1)
        }
      }

      'applyIllegalBufferSize - {
        intercept[IllegalArgumentException] {
          Handler(plusOne, 1.second, "processor", 0)
        }
      }

      'handlerInTime - {
        val timeout   = 100.milliseconds.dilated
        val processor = Handler((n: Int) => Future.successful(n + 1), timeout, "processor", 1, 1)

        Future
          .sequence(List(42, 43, 44, 45).map(processor.process))
          .map { responses =>
            assert {
              responses == List(43, 44, 45, 46)
            }
          }
      }

      'inTime - {
        val timeout   = 100.milliseconds.dilated
        val processor = Handler(plusOne, timeout, "processor", 1)

        Future
          .sequence(List(42, 43, 44, 45).map(processor.process))
          .map { responses =>
            assert {
              responses == List(43, 44, 45, 46)
            }
          }
      }

      'notInTime - {
        val timeout = 100.milliseconds.dilated
        val process = plusOne.via(
          Flow[(Int, Promise[Int])].delay(1.second.dilated, OverflowStrategy.backpressure)
        )
        val processor = Handler(process, timeout, "processor", 1)

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
        val timeout = 100.milliseconds.dilated
        val process =
          plusOne
            .grouped(2)
            .via(Flow[(Seq[Int], Seq[Promise[Int]])].mapConcat {
              case (Seq(n1, n2), Seq(p1, p2)) => List((n2, p2), (n1, p1))
            })
        val processor = Handler(process, timeout, "processor", 1)

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
        val processor = Handler(process, timeout, "processor", 1)

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
        val processor = Handler(process, timeout, "processor", 1)

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
        val timeout = 1000.milliseconds.dilated
        val process = plusOne.via(
          Flow[(Int, Promise[Int])].delay(100.milliseconds.dilated, OverflowStrategy.backpressure)
        )
        val processor = Handler(process, timeout, "processor", 1)

        val responses = Future.sequence(List(42, 43, 44, 45).map(processor.process))
        for {
          _  <- processor.shutdown()
          rs <- responses
        } yield assert(rs == List(43, 44, 45, 46))
      }

      'noLongerEnqueueOnShutdown - {
        val timeout = 100.milliseconds.dilated
        val process = plusOne.via(
          Flow[(Int, Promise[Int])].delay(100.milliseconds.dilated, OverflowStrategy.backpressure)
        )
        val processor = Handler(process, timeout, "processor", 1)

        processor.shutdown()
        Future
          .sequence(List(42, 43, 44, 45).map(processor.process).map(_.failed))
          .map { responses =>
            assert(responses.map(_ => "failure") == List.fill(4)("failure"))
          }
      }
    }
}
