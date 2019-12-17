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

import akka.actor.{ ActorSystem => ClassicSystem }
import akka.actor.CoordinatedShutdown.Reason
import akka.actor.typed.{ Behavior, Scheduler }
import akka.actor.typed.scaladsl.{ ActorContext, Behaviors }
import akka.actor.typed.scaladsl.adapter.{ ClassicActorSystemOps, TypedActorSystemOps }
import akka.cluster.typed.{
  Cluster,
  ClusterSingleton,
  SelfUp,
  SingletonActor,
  Subscribe,
  Unsubscribe
}
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.scaladsl.AkkaManagement
import akka.stream.Materializer
import io.moia.streamee.FrontProcessor
import org.apache.logging.log4j.core.async.AsyncLoggerContextSelector
import org.slf4j.LoggerFactory
import pureconfig.generic.auto.exportReader
import pureconfig.ConfigSource
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

object Main {

  final case class Config(
      api: Api.Config,
      textShufflerProcessorTimeout: FiniteDuration,
      textShuffler: TextShuffler.Config
  )

  final object TopLevelActorTerminated extends Reason

  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    // Always use async logging!
    sys.props += "log4j2.contextSelector" -> classOf[AsyncLoggerContextSelector].getName

    // Must happen before creating the actor system!
    val config = ConfigSource.default.at("streamee-demo").loadOrThrow[Config]

    // Always start with a classic system!
    val system = ClassicSystem("streamee-demo")
    system.spawn(Main(config), "main")

    // Cluster bootstrap
    AkkaManagement(system).start()
    ClusterBootstrap(system).start()
  }

  def apply(config: Config): Behavior[SelfUp] =
    Behaviors.setup { context =>
      if (logger.isInfoEnabled)
        logger.info(s"${context.system.name} started and ready to join cluster")
      Cluster(context.system).subscriptions ! Subscribe(context.self, classOf[SelfUp])

      Behaviors.receive { (context, _) =>
        if (logger.isInfoEnabled) logger.info(s"${context.system.name} joined cluster and is up")
        Cluster(context.system).subscriptions ! Unsubscribe(context.self)

        initialize(config)(context)

        Behaviors.empty
      }
    }

  private def initialize(config: Config)(implicit context: ActorContext[_]) = {
    import config._

    implicit val mat: Materializer            = Materializer(context.system)
    implicit val ec: ExecutionContext         = context.executionContext
    implicit val scheduler: Scheduler         = context.system.scheduler
    implicit val classicSystem: ClassicSystem = context.system.toClassic

    val wordShufflerRunner =
      ClusterSingleton(context.system).init(
        SingletonActor(WordShufflerRunner(), "word-shuffler")
          .withStopMessage(WordShufflerRunner.Shutdown)
      )

    val textShufflerProcessor =
      FrontProcessor(
        TextShuffler(config.textShuffler, wordShufflerRunner),
        textShufflerProcessorTimeout,
        "text-shuffler"
      )

    Api(config.api, textShufflerProcessor)
  }
}
