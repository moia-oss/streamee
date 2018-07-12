/*
 * Copyright (c) MOIA GmbH
 */

package io.moia.streamee

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.scaladsl.Flow
import akka.stream.typed.scaladsl.ActorMaterializer
import akka.stream.{ Materializer, OverflowStrategy, QueueOfferResult }
import io.moia.streamee.ExpiringPromise.PromiseExpired
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import utest._

object ProcessorTests extends TestSuite {

  private implicit val system: ActorSystem[Nothing] =
    ActorSystem(Behaviors.empty, "ExpiringPromiseTests")

  private implicit val mat: Materializer =
    ActorMaterializer()

  private val scheduler = system.scheduler

  private val toUpperCase = Flow[String].map(_.toUpperCase)

  import system.executionContext

  override def tests: Tests =
    Tests {
      'inTime - {
        val pipeline       = toUpperCase
        val (processor, _) = Processor(pipeline)

        val promise = ExpiringPromise[String](100.milliseconds, scheduler)
        processor.offer(("abc", promise))

        promise.future.map(s => assert(s == "ABC"))
      }

      'notInTime - {
        val pipeline       = toUpperCase.delay(1.second, OverflowStrategy.backpressure)
        val (processor, _) = Processor(pipeline)

        val timeout = 100.milliseconds
        val promise = ExpiringPromise[String](timeout, scheduler)

        Future.sequence(
          List(
            processor.offer(("abc", promise)).map(r => assert(r == QueueOfferResult.Enqueued)),
            promise.future.failed.map(t => assert(t == PromiseExpired(timeout)))
          )
        )
      }

      'processInFlightOnShutdown - {
        val pipeline                    = toUpperCase.delay(100.milliseconds, OverflowStrategy.backpressure)
        val (processor, shutdownSwitch) = Processor(pipeline)

        val timeout  = 500.milliseconds
        val promise1 = ExpiringPromise[String](timeout, scheduler)
        val promise2 = ExpiringPromise[String](timeout, scheduler)
        val promise3 = ExpiringPromise[String](timeout, scheduler)
        val promise4 = ExpiringPromise[String](timeout, scheduler)

        for {
          _ <- processor.offer(("abc", promise1))
          _ <- processor.offer(("def", promise2))
          _ <- processor.offer(("ghi", promise3))
          _ <- processor.offer(("jkl", promise4))
        } shutdownSwitch.shutdown()

        Future.sequence(
          List(
            promise1.future.map(s => assert(s == "ABC")),
            promise2.future.map(s => assert(s == "DEF")),
            promise3.future.map(s => assert(s == "GHI")),
            promise4.future.map(s => assert(s == "JKL"))
          )
        )
      }

      'noLongerEnqueueOnShutdown - {
        val pipeline                    = toUpperCase.delay(100.milliseconds, OverflowStrategy.backpressure)
        val (processor, shutdownSwitch) = Processor(pipeline)

        val timeout  = 500.milliseconds
        val promise1 = ExpiringPromise[String](timeout, scheduler)
        val promise2 = ExpiringPromise[String](timeout, scheduler)
        val promise3 = ExpiringPromise[String](timeout, scheduler)
        val promise4 = ExpiringPromise[String](timeout, scheduler)

        for {
          _ <- processor.offer(("abc", promise1))
          _ <- processor.offer(("def", promise2))
          _ = shutdownSwitch.shutdown() // Don't use a generator (<-) here!
          _ <- processor.offer(("ghi", promise3))
          _ <- processor.offer(("jkl", promise4))
        } ()

        Future.sequence(
          List(
            promise1.future.map(s => assert(s == "ABC")),
            promise2.future.map(s => assert(s == "DEF")),
            promise3.future.failed.map(t => assert(t == PromiseExpired(timeout))),
            promise4.future.failed.map(t => assert(t == PromiseExpired(timeout)))
          )
        )
      }
    }

  override def utestAfterAll(): Unit = {
    system.terminate()
    super.utestAfterAll()
  }
}
