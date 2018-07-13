/*
 * Copyright (c) MOIA GmbH
 */

package io.moia.streamee
package demo

import akka.actor.CoordinatedShutdown.Reason
import akka.actor.typed.scaladsl.{ ActorContext, Behaviors }
import akka.actor.typed.{ ActorSystem, Behavior, Terminated }
import akka.actor.{ ActorSystem => UntypedSystem, CoordinatedShutdown, Scheduler }
import akka.cluster.typed.{ Cluster, SelfUp, Subscribe }
import akka.management.AkkaManagement
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.stream.Materializer
import akka.stream.typed.scaladsl.ActorMaterializer
import org.apache.logging.log4j.core.async.AsyncLoggerContextSelector
import org.apache.logging.log4j.scala.Logging
import pureconfig.loadConfigOrThrow

/**
  * Runner for this demo. Creates actor system, API, etc.
  */
object Main extends Logging {
  import akka.actor.typed.scaladsl.adapter._

  final case class Config(api: Api.Config)

  final object TopLevelActorTerminated extends Reason

  def main(args: Array[String]): Unit = {
    sys.props += "log4j2.contextSelector" -> classOf[AsyncLoggerContextSelector].getName // Always use async logging!

    val config = loadConfigOrThrow[Config]("streamee-demo") // Must be first!
    val system = ActorSystem(Main(config), "streamee-demo")

    AkkaManagement(system.toUntyped).start()
    ClusterBootstrap(system.toUntyped).start()

    logger.info(s"${system.name} started and ready to join cluster")
  }

  def apply(config: Config): Behavior[SelfUp] =
    Behaviors.setup { context =>
      Cluster(context.system).subscriptions ! Subscribe(context.self, classOf[SelfUp])

      Behaviors
        .receiveMessage[SelfUp] { _ =>
          logger.info(s"${context.system.name} joined cluster and is up")
          onSelfUp(config, context)
          Behaviors.empty
        }
        .receiveSignal {
          case (_, Terminated(actor)) =>
            logger.error(s"Shutting down, because $actor terminated!")
            CoordinatedShutdown(context.system.toUntyped).run(TopLevelActorTerminated)
            Behaviors.same
        }
    }

  private def onSelfUp(config: Config, context: ActorContext[SelfUp]) = {
    implicit val untypedSystem: UntypedSystem = context.system.toUntyped
    implicit val mat: Materializer            = ActorMaterializer()(context.system)
    implicit val scheduler: Scheduler         = context.system.scheduler

    val demoProcessor =
      Processor(DemoPipeline(scheduler)(untypedSystem.dispatcher),
                parallelsim = 42,
                CoordinatedShutdown(context.system.toUntyped))
    Api(config.api, demoProcessor)
  }
}
