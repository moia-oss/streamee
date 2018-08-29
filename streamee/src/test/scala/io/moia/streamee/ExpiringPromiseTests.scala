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

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import scala.concurrent.duration.DurationInt
import utest._

object ExpiringPromiseTests extends TestSuite {
  import ExpiringPromise._

  private val system: ActorSystem[Nothing] =
    ActorSystem(Behaviors.empty, getClass.getSimpleName.init)

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
    system.terminate()
    super.utestAfterAll()
  }
}
