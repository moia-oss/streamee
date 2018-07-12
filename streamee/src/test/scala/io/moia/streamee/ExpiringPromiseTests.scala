/*
 * Copyright (c) MOIA GmbH
 */

package io.moia.streamee

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import scala.concurrent.duration.DurationInt
import utest._

object ExpiringPromiseTests extends TestSuite {
  import ExpiringPromise._

  private val system: ActorSystem[Nothing] =
    ActorSystem(Behaviors.empty, "ExpiringPromiseTests")

  private val scheduler = system.scheduler

  import system.executionContext

  override def tests: Tests =
    Tests {
      'expire - {
        val timeout = 100.milliseconds
        val promise = ExpiringPromise[String](timeout, scheduler)

        promise.future.failed.map(t => assert(t == PromiseExpired(timeout)))
      }

      'expireNot - {
        val timeout = 100.milliseconds
        val promise = ExpiringPromise[String](timeout, scheduler)

        val success = "success"
        promise.trySuccess(success)
        promise.future.map(s => assert(s == success))
      }
    }

  override def utestAfterAll(): Unit = {
    super.utestAfterAll()
    system.terminate()
  }
}
