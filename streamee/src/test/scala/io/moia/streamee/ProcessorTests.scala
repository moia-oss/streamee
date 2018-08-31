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

import akka.actor.{ CoordinatedShutdown, ActorSystem => UntypedSystem }
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter.TypedActorSystemOps
import akka.pattern.after
import akka.stream.{ Materializer, OverflowStrategy, QueueOfferResult }
import akka.stream.scaladsl.{ Flow, SourceQueueWithComplete }
import akka.stream.typed.scaladsl.ActorMaterializer
import akka.testkit.TestDuration
import io.moia.streamee.ExpiringPromise.PromiseExpired
import io.moia.streamee.Processor.NotCorrelated
import scala.concurrent.{ Future, Promise }
import scala.concurrent.duration.DurationInt
import utest._

object ProcessorTests extends TestSuite {

  private implicit val system: ActorSystem[Nothing] =
    ActorSystem(Behaviors.empty, getClass.getSimpleName.init)

  private implicit val untypedSystem: UntypedSystem =
    system.toUntyped

  private implicit val mat: Materializer =
    ActorMaterializer()

  private val scheduler = system.scheduler

  private val shutdown = CoordinatedShutdown(system.toUntyped)

  private val plusOne = Flow[Int].map(_ + 1)

  import system.executionContext

  override def tests: Tests =
    Tests {
      'inTime - {
        val processor = Processor(plusOne, ProcessorSettings(system), shutdown)

        val promise = ExpiringPromise[Int](100.milliseconds.dilated, scheduler)
        processor.offer((42, promise))

        promise.future.map(s => assert(s == 43))
      }

      'notInTime - {
        val process   = plusOne.delay(1.second, OverflowStrategy.backpressure)
        val processor = Processor(process, ProcessorSettings(system), shutdown)

        val timeout = 100.milliseconds.dilated
        val promise = ExpiringPromise[Int](timeout, scheduler)

        Future.sequence(
          List(
            processor.offer((42, promise)).map(r => assert(r == QueueOfferResult.Enqueued)),
            promise.future.failed.map(t => assert(t == PromiseExpired(timeout)))
          )
        )
      }

      'processInFlightOnShutdown - {
        val process   = plusOne.delay(100.milliseconds.dilated, OverflowStrategy.backpressure)
        val processor = Processor(process, ProcessorSettings(system), shutdown)

        val timeout  = 500.milliseconds.dilated
        val promise1 = ExpiringPromise[Int](timeout, scheduler)
        val promise2 = ExpiringPromise[Int](timeout, scheduler)
        val promise3 = ExpiringPromise[Int](timeout, scheduler)
        val promise4 = ExpiringPromise[Int](timeout, scheduler)

        for {
          _ <- processor.offer((42, promise1))
          _ <- processor.offer((43, promise2))
          _ <- processor.offer((44, promise3))
          _ <- processor.offer((45, promise4))
        } processor.asInstanceOf[SourceQueueWithComplete[(Int, Promise[Int])]].complete()

        Future.sequence(
          List(
            promise1.future.map(s => assert(s == 43)),
            promise2.future.map(s => assert(s == 44)),
            promise3.future.map(s => assert(s == 45)),
            promise4.future.map(s => assert(s == 46))
          )
        )
      }

      'noLongerEnqueueOnShutdown - {
        val process   = plusOne.delay(100.milliseconds.dilated, OverflowStrategy.backpressure)
        val processor = Processor(process, ProcessorSettings(system), shutdown)

        val timeout  = 500.milliseconds.dilated
        val promise1 = ExpiringPromise[Int](timeout, scheduler)
        val promise2 = ExpiringPromise[Int](timeout, scheduler)
        val promise3 = ExpiringPromise[Int](timeout, scheduler)
        val promise4 = ExpiringPromise[Int](timeout, scheduler)

        for {
          _ <- processor.offer((42, promise1))
          _ <- processor.offer((43, promise2))
          _ = processor.asInstanceOf[SourceQueueWithComplete[(Int, Promise[Int])]].complete()
          _ <- processor.offer((44, promise3))
          _ <- processor.offer((45, promise4))
        } ()

        Future.sequence(
          List(
            promise1.future.map(s => assert(s == 43)),
            promise2.future.map(s => assert(s == 44)),
            promise3.future.failed.map(t => assert(t == PromiseExpired(timeout))),
            promise4.future.failed.map(t => assert(t == PromiseExpired(timeout)))
          )
        )
      }

      'notCorrelatedFilter - {
        val process = plusOne.filter(_ % 2 != 0)
        val processor =
          Processor(process, ProcessorSettings(system), shutdown, (c: Int, r: Int) => c + 1 == r)

        val timeout  = 100.milliseconds.dilated
        val promise1 = ExpiringPromise[Int](timeout, scheduler)
        val promise2 = ExpiringPromise[Int](timeout, scheduler)
        val promise3 = ExpiringPromise[Int](timeout, scheduler)
        processor.offer((42, promise1))
        processor.offer((43, promise2))
        processor.offer((44, promise3))

        Future.sequence(
          List(
            promise1.future.map(s => assert(s == 43)),
            promise2.future.failed.map(t => assert(t == NotCorrelated(43, 45))),
            promise3.future.failed.map(t => assert(t == PromiseExpired(timeout)))
          )
        )
      }

      'notCorrelatedUnordered - {
        val process =
          plusOne.mapAsyncUnordered(42) { n =>
            if (n % 2 != 0)
              Future.successful(n)
            else
              after(100.milliseconds.dilated, scheduler)(Future.successful(n))
          }
        val processor =
          Processor(process, ProcessorSettings(system), shutdown, (c: Int, r: Int) => c + 1 == r)

        val timeout  = 500.milliseconds.dilated
        val promise1 = ExpiringPromise[Int](timeout, scheduler)
        val promise2 = ExpiringPromise[Int](timeout, scheduler)
        val promise3 = ExpiringPromise[Int](timeout, scheduler)
        processor.offer((42, promise1))
        processor.offer((43, promise2))
        processor.offer((44, promise3))

        Future.sequence(
          List(
            promise1.future.map(s => assert(s == 43)),
            promise2.future.failed.map(t => assert(t == NotCorrelated(43, 45))),
            promise3.future.failed.map(t => assert(t == NotCorrelated(44, 44)))
          )
        )
      }
    }

  override def utestAfterAll(): Unit = {
    system.terminate()
    super.utestAfterAll()
  }
}
