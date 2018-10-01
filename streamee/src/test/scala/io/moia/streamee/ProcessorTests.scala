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

import akka.stream.{ ActorAttributes, DelayOverflowStrategy, OverflowStrategy, Supervision }
import akka.stream.scaladsl.Flow
import akka.testkit.TestDuration
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import utest._

object ProcessorTests extends ActorTestSuite {
  import testKit._

  private val plusOne = Flow[Int].map(_ + 1)

  private val settings = ProcessorSettings(system)

  override def tests: Tests =
    Tests {
      'inTime - {
        val processor = Processor(plusOne, "processor", settings)(identity, _ - 1)
        val timeout   = 100.milliseconds.dilated

        Future
          .sequence(
            List(
              processor.process(42, timeout),
              processor.process(43, timeout),
              processor.process(44, timeout),
              processor.process(45, timeout)
            )
          )
          .map { responses =>
            assert(responses == List(43, 44, 45, 46))
          }
      }

      'notInTime - {
        val process   = plusOne.delay(1.second.dilated, OverflowStrategy.backpressure)
        val processor = Processor(process, "processor", settings)(identity, _ - 1)
        val timeout   = 100.milliseconds.dilated

        Future
          .sequence(
            List(
              processor.process(42, timeout),
              processor.process(43, timeout),
              processor.process(44, timeout),
              processor.process(45, timeout)
            ).map(_.failed)
          )
          .map { responses =>
            assert(
              responses == List(PromiseExpired(timeout, "42"),
                                PromiseExpired(timeout, "43"),
                                PromiseExpired(timeout, "44"),
                                PromiseExpired(timeout, "45"))
            )
          }
      }

      'reorder - {
        val process   = plusOne.grouped(2).mapConcat { case Seq(n1, n2) => List(n2, n1) }
        val processor = Processor(process, "processor", settings)(identity, _ - 1)
        val timeout   = 100.milliseconds.dilated

        Future
          .sequence(
            List(
              processor.process(42, timeout),
              processor.process(43, timeout),
              processor.process(44, timeout),
              processor.process(45, timeout)
            )
          )
          .map { responses =>
            assert(responses == List(43, 44, 45, 46))
          }
      }

      'filter - {
        val process   = plusOne.filter(_ % 2 != 0)
        val processor = Processor(process, "processor", settings)(identity, _ - 1)
        val timeout   = 100.milliseconds.dilated

        Future
          .sequence(
            List(
              processor.process(42, timeout),
              processor.process(43, timeout).failed,
              processor.process(44, timeout),
              processor.process(45, timeout).failed
            )
          )
          .map { responses =>
            assert(
              responses == List(43,
                                PromiseExpired(timeout, "43"),
                                45,
                                PromiseExpired(timeout, "45"))
            )
          }
      }

      'resume - {
        val process =
          plusOne
            .map(n => if (n % 2 == 0) throw new Exception("boom") else n)
            .withAttributes(ActorAttributes.supervisionStrategy(_ => Supervision.Resume))
        val processor = Processor(process, "processor", settings)(identity, _ - 1)
        val timeout   = 100.milliseconds.dilated

        Future
          .sequence(
            List(
              processor.process(42, timeout),
              processor.process(43, timeout).failed,
              processor.process(44, timeout),
              processor.process(45, timeout).failed
            )
          )
          .map { responses =>
            assert(
              responses == List(43,
                                PromiseExpired(timeout, "43"),
                                45,
                                PromiseExpired(timeout, "45"))
            )
          }
      }

      'processInFlightOnShutdown - {
        val process   = plusOne.delay(100.milliseconds.dilated, DelayOverflowStrategy.backpressure)
        val processor = Processor(process, "processor", settings)(identity, _ - 1)
        val timeout   = 1000.milliseconds.dilated

        val responses =
          Future
            .sequence(
              List(
                processor.process(42, timeout),
                processor.process(43, timeout),
                processor.process(44, timeout),
                processor.process(45, timeout),
              )
            )
        for {
          _  <- processor.shutdown()
          rs <- responses
        } yield assert(rs == List(43, 44, 45, 46))
      }

      'noLongerEnqueueOnShutdown - {
        val process   = plusOne.delay(100.milliseconds.dilated, DelayOverflowStrategy.backpressure)
        val processor = Processor(process, "processor", settings)(identity, _ - 1)
        val timeout   = 100.milliseconds.dilated

        processor.shutdown()
        Future
          .sequence(
            List(
              processor.process(42, timeout).failed,
              processor.process(43, timeout).failed,
              processor.process(44, timeout).failed,
              processor.process(45, timeout).failed,
            )
          )
          .map { responses =>
            assert(responses.map(_ => "failure") == List.fill(4)("failure"))
          }
      }

      'swipe - {
        val process =
          plusOne
            .buffer(100, OverflowStrategy.backpressure)
            .delay(500.milliseconds.dilated, OverflowStrategy.backpressure)
        val settings  = PlainProcessorSettings(1, 100.milliseconds.dilated)
        val processor = Processor(process, "processor", settings)(identity, _ - 1)
        val timeout   = 100.milliseconds.dilated

        val responses = 1.to(100).map(n => processor.process(n, timeout).failed)
        Future
          .sequence(responses)
          .map(_.map(_.getClass.getSimpleName))
          .map(rs => assert(rs == List.fill(100)(classOf[PromiseExpired].getSimpleName)))
      }
    }
}
