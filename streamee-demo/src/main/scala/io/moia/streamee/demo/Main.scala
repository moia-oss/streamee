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

package io.moia.streamee.demo

import akka.actor.{ Scheduler, ActorSystem => UntypedSystem }
import akka.actor.CoordinatedShutdown.Reason
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ ActorContext, Behaviors }
import akka.actor.typed.scaladsl.adapter.{ TypedActorSystemOps, UntypedActorSystemOps }
import akka.cluster.typed.{ Cluster, SelfUp, Subscribe, Unsubscribe }
import akka.stream.Materializer
import akka.stream.typed.scaladsl.ActorMaterializer
import org.apache.logging.log4j.core.async.AsyncLoggerContextSelector
import org.apache.logging.log4j.scala.Logging
import pureconfig.generic.auto.exportReader
import pureconfig.loadConfigOrThrow
import scala.concurrent.ExecutionContext

object Main extends Logging {

  final case class Config(api: Api.Config, wordShufflerHandler: WordShufflerHandler.Config)

  final object TopLevelActorTerminated extends Reason

  def main(args: Array[String]): Unit = {
    sys.props += "log4j2.contextSelector" -> classOf[AsyncLoggerContextSelector].getName // Always use async logging!
    val config = loadConfigOrThrow[Config]("streamee-demo") // Must be first!
    val system = UntypedSystem("streamee-demo")             // Always start with an untyped system!
    system.spawn(Main(config), "main")
  }

  def apply(config: Config): Behavior[SelfUp] =
    Behaviors.setup { context =>
      logger.info(s"${context.system.name} started and ready to join cluster")
      Cluster(context.system).subscriptions ! Subscribe(context.self, classOf[SelfUp])

      Behaviors.receive { (context, _) =>
        logger.info(s"${context.system.name} joined cluster and is up")
        Cluster(context.system).subscriptions ! Unsubscribe(context.self)

        initialize(config)(context)

        Behaviors.empty
      }
    }

  private def initialize(config: Config)(implicit context: ActorContext[_]) = {
    implicit val mat: Materializer            = ActorMaterializer()(context.system)
    implicit val ec: ExecutionContext         = context.executionContext
    implicit val scheduler: Scheduler         = context.system.scheduler
    implicit val untypedSystem: UntypedSystem = context.system.toUntyped

    val wordShufflerHandler = WordShufflerHandler(config.wordShufflerHandler)

    Api(config.api, wordShufflerHandler)
  }
}
