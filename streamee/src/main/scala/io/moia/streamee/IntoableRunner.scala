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
import akka.stream.{ KillSwitches, Materializer, SinkRef }
import akka.stream.scaladsl.{ Flow, Keep, MergeHub, Sink, StreamRefs }
import org.apache.logging.log4j.scala.Logging
import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration
import scala.util.{ Failure, Success }

/**
  * Runs an "intoable" process, i.e. a `Flow` which takes pairs of request and response promises.
  */
object IntoableRunner extends Logging {

  sealed trait Command
  final case class GetSinkRef[A, B](replyTo: ActorRef[SinkRef[(A, ActorRef[Respondee.Command[B]])]])
      extends Command
  final case object Shutdown     extends Command
  private final case object Stop extends Command

  /**
    * Manages an "intoable" process, i.e. a `Flow` which takes pairs of request and response promise
    * and produces pairs of response and the response promise (expected to be threaded through). The
    * process is run with a sink that completes the threaded through promises and a merge hub source
    * which allows for attaching from another stream, e.g. conveniantly via
    * [[io.moia.streamee.SourceOps.into]] or by obtaining a `SinkRef` via sending `GetSinkRef`.
    *
    * The runner registers with Akka Coordinated Shutdown such that it gracefully completes all
    * in-flight requests. During shutdown no more `SinkRef`s can be obtained, i.e. clients have to
    * retry hoping for another runner instance to come up, e.g. when using Akka Cluster Sharding.
    */
  def apply[A, B](process: Flow[(A, ActorRef[Respondee.Command[B]]),
                                (B, ActorRef[Respondee.Command[B]]),
                                NotUsed],
                  shutdown: CoordinatedShutdown)(implicit mat: Materializer): Behavior[Command] =
    Behaviors.setup { context =>
      import context.executionContext

      val self = context.self

      val ((sink, switch), done) =
        MergeHub
          .source[(A, ActorRef[Respondee.Command[B]])]
          .viaMat(KillSwitches.single)(Keep.both)
          .via(process)
          .toMat(Sink.foreach { case (b, r) => r ! Respondee.Response(b) })(Keep.both)
          .run()

      shutdown.addTask(CoordinatedShutdown.PhaseServiceStop, "intoable-runner") { () =>
        self ! Shutdown
        done
      }

      done.onComplete(_ => self ! Stop)

      Behaviors.receiveMessage {
        case GetSinkRef(
            replyTo: ActorRef[SinkRef[(A, ActorRef[Respondee.Command[B]])]] @unchecked
            ) =>
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
