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

import akka.NotUsed
import akka.actor.CoordinatedShutdown
import akka.actor.typed.{ ActorRef, Behavior }
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.scaladsl.{
  ClusterSharding,
  EntityRef,
  EntityTypeKey,
  ShardedEntity
}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.stream.{ KillSwitches, Materializer, SinkRef }
import akka.stream.scaladsl.{ Flow, Keep, MergeHub, Sink, StreamRefs }
import org.apache.logging.log4j.scala.Logging
import scala.concurrent.Promise
import scala.util.{ Failure, Success }

object IntoableRunner extends Logging {

  sealed trait Command
  final case class GetSinkRef[A, B](replyTo: ActorRef[SinkRef[(A, Promise[B])]]) extends Command
  final case object Shutdown                                                     extends Command
  private final case object Stop                                                 extends Command

  def apply[A, B](process: Flow[(A, Promise[B]), (B, Promise[B]), NotUsed],
                  shutdown: CoordinatedShutdown)(implicit mat: Materializer): Behavior[Command] =
    Behaviors.setup { context =>
      import context.executionContext

      val self = context.self

      val ((sink, switch), done) =
        MergeHub
          .source[(A, Promise[B])]
          .viaMat(KillSwitches.single)(Keep.both)
          .via(process)
          .toMat(Sink.foreach { case (b, p) => p.trySuccess(b) })(Keep.both)
          .run()

      shutdown.addTask(CoordinatedShutdown.PhaseServiceStop, "runner") { () =>
        self ! Shutdown
        done
      }

      done.onComplete(_ => self ! Stop)

      Behaviors.receiveMessage {
        case GetSinkRef(replyTo: ActorRef[SinkRef[(A, Promise[B])]] @unchecked) =>
          StreamRefs.sinkRef().to(sink).run().onComplete {
            case Failure(cause)   => logger.error("Cannot create SinkRef!", cause)
            case Success(sinkRef) => replyTo ! sinkRef
          }
          Behaviors.same

        case Shutdown =>
          logger.info("Shutdown requested")
          switch.shutdown()

          Behaviors.receiveMessagePartial {
            case Stop =>
              logger.info("Stopping because shutdown completed")
              Behaviors.stopped
          }

        case Stop =>
          logger.info("Stopping because process completed")
          Behaviors.stopped
      }
    }
}

object IntoableRunnerSharding {
  import IntoableRunner._

  val entityKey: EntityTypeKey[Command] =
    EntityTypeKey[Command](s"intoable-runner")

  def start[A, B](process: Flow[(A, Promise[B]), (B, Promise[B]), NotUsed],
                  sharding: ClusterSharding,
                  shutdown: CoordinatedShutdown)(
      implicit mat: Materializer
  ): (ActorRef[ShardingEnvelope[Command]], String => EntityRef[GetSinkRef[A, B]]) = {
    val region =
      sharding.start(ShardedEntity(_ => IntoableRunner(process, shutdown), entityKey, Shutdown))
    (region, sharding.entityRefFor(entityKey, _).asInstanceOf[EntityRef[GetSinkRef[A, B]]])
  }
}
