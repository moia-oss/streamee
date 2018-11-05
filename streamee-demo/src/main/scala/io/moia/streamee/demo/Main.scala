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
package demo

import akka.actor.{ CoordinatedShutdown, Scheduler }
import akka.actor.CoordinatedShutdown.Reason
import akka.actor.typed.{ ActorRef, ActorSystem, Behavior }
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter.TypedActorSystemOps
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.typed.{ Cluster, SelfUp, Subscribe, Unsubscribe }
import akka.management.AkkaManagement
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.stream.Materializer
import akka.stream.typed.scaladsl.ActorMaterializer
import io.moia.streamee.intoable.RespondeeFactory
import org.apache.logging.log4j.core.async.AsyncLoggerContextSelector
import pureconfig.generic.auto.exportReader
import pureconfig.loadConfigOrThrow
import scala.concurrent.ExecutionContext

/**
  * Runner for this demo. Creates actor system, API, etc.
  */
object Main {

  final case class Config(api: Api.Config,
                          length: Length.Config,
                          delayedLengthSharding: DelayedLengthSharding.Config)

  final object TopLevelActorTerminated extends Reason

  def main(args: Array[String]): Unit = {
    sys.props += "log4j2.contextSelector" -> classOf[AsyncLoggerContextSelector].getName // Always use async logging!

    val config = loadConfigOrThrow[Config]("streamee-demo") // Must be first!
    val system = ActorSystem(Main(config), "streamee-demo")

    AkkaManagement(system.toUntyped).start()
    ClusterBootstrap(system.toUntyped).start()
  }

  def apply(config: Config): Behavior[SelfUp] =
    Behaviors.setup { context =>
      context.log.info("{} started and ready to join cluster", context.system.name)

      Cluster(context.system).subscriptions ! Subscribe(context.self, classOf[SelfUp])

      Behaviors.receive { (context, _) =>
        context.log.info("{} joined cluster and is up", context.system.name)

        Cluster(context.system).subscriptions ! Unsubscribe(context.self)

        implicit val system: ActorSystem[_] = context.system
        implicit val mat: Materializer      = ActorMaterializer()(context.system)
        implicit val ec: ExecutionContext   = context.executionContext
        implicit val scheduler: Scheduler   = context.system.scheduler

        implicit val intRespondeeFactory: ActorRef[RespondeeFactory.Command[Int]] =
          context.spawn(RespondeeFactory[Int](), "int-respondee-factory")

        val fourtyTwo = FourtyTwo()

        val delayedLengthFor =
          DelayedLengthSharding(config.delayedLengthSharding,
                                DelayedLength(),
                                ClusterSharding(system),
                                CoordinatedShutdown(system.toUntyped))
        val length = Length(config.length, delayedLengthFor)

        Api(config.api, fourtyTwo, length)

        Behaviors.empty
      }
    }
}
