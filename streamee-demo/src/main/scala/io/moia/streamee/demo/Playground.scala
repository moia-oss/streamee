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

import akka.NotUsed
import akka.actor.{ CoordinatedShutdown, Scheduler, ActorSystem => UntypedSystem }
import akka.actor.typed.{ ActorRef, ActorSystem, Behavior }
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter.TypedActorSystemOps
import akka.cluster.sharding.typed.scaladsl.{
  ClusterSharding,
  EntityRef,
  EntityTypeKey,
  ShardedEntity
}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.typed.{ Cluster, SelfUp, Subscribe, Unsubscribe }
import akka.event.LoggingAdapter
import akka.http.scaladsl.Http
import akka.stream.{ DelayOverflowStrategy, KillSwitches, Materializer, SinkRef, ThrottleMode }
import akka.stream.scaladsl.{ Flow, Keep, MergeHub, Sink, Source, StreamRefs }
import akka.stream.typed.scaladsl.ActorMaterializer
import akka.util.Timeout
import org.apache.logging.log4j.core.async.AsyncLoggerContextSelector
import org.apache.logging.log4j.scala.Logging
import pureconfig.loadConfigOrThrow
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration.DurationInt
import scala.util.{ Failure, Success }

object Poc {

  final case class Config(api: Api.Config)

  def main(args: Array[String]): Unit = {
    sys.props += "log4j2.contextSelector" -> classOf[AsyncLoggerContextSelector].getName // Always use async logging!
    val config = loadConfigOrThrow[Config]("streamee-demo") // Must be first!
    ActorSystem(Poc(config), "streamee-demo")
  }

  def apply(config: Config): Behavior[SelfUp] =
    Behaviors.setup { context =>
      context.log.info("{} started and ready to join cluster", context.system.name)

      Cluster(context.system).subscriptions ! Subscribe(context.self, classOf[SelfUp])

      Behaviors.receive { (context, _) =>
        context.log.info("{} joined cluster and is up", context.system.name)

        Cluster(context.system).subscriptions ! Unsubscribe(context.self)

        implicit val system: ActorSystem[_]       = context.system
        implicit val untypedSystem: UntypedSystem = system.toUntyped
        implicit val mat: Materializer            = ActorMaterializer()(context.system)
        implicit val ec: ExecutionContext         = context.executionContext
        implicit val scheduler: Scheduler         = context.system.scheduler

        implicit val log: LoggingAdapter = akka.event.Logging(system.toUntyped, getClass.getName)
        val process =
          Flow[(Int, Promise[Int])]
            .throttle(1, 1.second, 1, ThrottleMode.Shaping)
            .log("1: ")
            .delay(1.second, DelayOverflowStrategy.backpressure)
            .log("2: ")
            .delay(1.second, DelayOverflowStrategy.backpressure)
            .log("3: ")
            .delay(1.second, DelayOverflowStrategy.backpressure)
            .log("4: ")

        Runner.startSharding(process,
                             ClusterSharding(system),
                             CoordinatedShutdown(system.toUntyped))
        val runnerFor                                                   = Runner.runnerFor(ClusterSharding(system)) _
        def getSinkRef(replyTo: ActorRef[SinkRef[(Int, Promise[Int])]]) = Runner.GetSinkRef(replyTo)

        val route = {
          import akka.http.scaladsl.server.Directives._
          pathSingleSlash {
            get {
              complete {
                val response =
                  Source
                    .single(42)
                    .mapAsync(1) { n =>
                      implicit val askTimeout: Timeout = 30.seconds
                      val sinkRef                      = runnerFor("id-1") ? getSinkRef
                      Future.successful(n).zip(sinkRef)
                    }
                    .mapAsync(1) {
                      case (n, sinkRef) =>
                        val p = Promise[Int]()
                        Source.single((n, p)).runWith(sinkRef)
                        p.future
                    }
                    .map(_.toString)
                    .runWith(Sink.head)
                response
              }
            }
          }
        }

        Http().bindAndHandle(route, "0.0.0.0", 8080)

        Behaviors.empty
      }
    }
}

object Runner extends Logging {

  sealed trait Command
  final case class GetSinkRef[A, B](replyTo: ActorRef[SinkRef[(A, Promise[B])]]) extends Command
  final case object Shutdown                                                     extends Command
  private final case object Stop                                                 extends Command

  val entityKey: EntityTypeKey[Command] =
    EntityTypeKey[Command](s"runner")

  def startSharding[A, B](
      process: Flow[(A, Promise[B]), (B, Promise[B]), NotUsed],
      sharding: ClusterSharding,
      shutdown: CoordinatedShutdown
  )(implicit mat: Materializer): ActorRef[ShardingEnvelope[Command]] =
    sharding.start(ShardedEntity(_ => Runner(process, shutdown), entityKey, Shutdown))

  def runnerFor(sharding: ClusterSharding)(id: String): EntityRef[Command] =
    sharding.entityRefFor(entityKey, id)

  def apply[A, B](process: Flow[(A, Promise[B]), (B, Promise[B]), NotUsed],
                  shutdown: CoordinatedShutdown)(implicit mat: Materializer): Behavior[Command] =
    Behaviors.setup { context =>
      import context.executionContext

      val ((sink, switch), done) =
        MergeHub
          .source[(A, Promise[B])]
          .viaMat(KillSwitches.single)(Keep.both)
          .via(process)
          .toMat(Sink.foreach { case (b, p) => p.trySuccess(b) })(Keep.both)
          .run()

      shutdown.addTask(CoordinatedShutdown.PhaseServiceStop, "runner") { () =>
        switch.shutdown()
        done
      }

      Behaviors.receivePartial {
        case (_, GetSinkRef(replyTo: ActorRef[SinkRef[(A, Promise[B])]] @unchecked)) =>
          StreamRefs.sinkRef().to(sink).run().onComplete {
            case Failure(cause)   => logger.error("Cannot create SinkRef!", cause)
            case Success(sinkRef) => replyTo ! sinkRef
          }
          Behaviors.same

        case (context, Shutdown) =>
          logger.info("Shutdown requested")
          val self = context.self
          switch.shutdown()
          done.onComplete(_ => self ! Stop)

          Behaviors.receiveMessagePartial {
            case Stop =>
              logger.info("Shutdown completed")
              Behaviors.stopped
          }
      }
    }
}
