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

import akka.actor.{ Scheduler, ActorSystem => UntypedSystem }
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.scaladsl.adapter.TypedActorSystemOps
import akka.stream.Materializer
import akka.stream.typed.scaladsl.ActorMaterializer
import org.scalatest.{ BeforeAndAfterAll, TestSuite }

trait ActorTestSuite extends TestSuite with BeforeAndAfterAll {

  protected val testKit: ActorTestKit =
    ActorTestKit()

  import testKit._

  protected implicit val untypedSystem: UntypedSystem =
    system.toUntyped

  protected implicit val mat: Materializer =
    ActorMaterializer()

  protected implicit val scheduler: Scheduler =
    system.scheduler

  override protected def afterAll(): Unit = {
    shutdownTestKit()
    super.afterAll()
  }
}
