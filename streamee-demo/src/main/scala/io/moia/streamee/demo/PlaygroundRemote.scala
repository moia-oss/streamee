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

import akka.actor.{ ActorSystem, CoordinatedShutdown }
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ ActorRef, Behavior }
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.scaladsl.{ ClusterSharding, EntityTypeKey, ShardedEntity }
import akka.stream.{ ActorMaterializer, DelayOverflowStrategy, Materializer, SinkRef, ThrottleMode }
import akka.stream.scaladsl.{ Flow, Keep, RestartSink, Sink, Source, StreamRefs }
import akka.util.Timeout
import io.moia.streamee.intoable._
import org.apache.logging.log4j.scala.Logging
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration.DurationInt
import scala.util.{ Failure, Success }

object PlaygroundRemote {

  object Runner extends Logging {
    sealed trait Command
    final case class GetSinkRef[A, B](
        replyTo: ActorRef[SinkRef[(A, ActorRef[Respondee.Command[B]])]]
    ) extends Command
    final case object Shutdown     extends Command
    private final case object Stop extends Command
    //SinkRef[(A, ActorRef[Respondee.Command[B]])]

    def apply[A, B](
        intoableProcess: Flow[(A, ActorRef[Respondee.Command[B]]),
                              (B, ActorRef[Respondee.Command[B]]),
                              Any],
        shutdown: CoordinatedShutdown
    )(implicit mat: Materializer): Behavior[Command] =
      Behaviors.setup { context =>
        val self                         = context.self
        val (intoableSink, switch, done) = runRemotelyIntoableFlow(intoableProcess, 42)

        done.onComplete(_ => self ! Stop)(context.executionContext)

        shutdown.addTask(CoordinatedShutdown.PhaseServiceStop, "runner") { () =>
          self ! Shutdown
          done
        }

        Behaviors.receive {
          case (context,
                GetSinkRef(
                  replyTo: ActorRef[SinkRef[(A, ActorRef[Respondee.Command[B]])]] @unchecked
                )) =>
            StreamRefs
              .sinkRef()
              .to(intoableSink)
              .run()
              .onComplete {
                case Failure(cause)   => logger.error("Cannot create SinkRef!", cause)
                case Success(sinkRef) => replyTo ! sinkRef
              }(context.executionContext)
            Behaviors.same

          case (_, Shutdown) =>
            logger.info("Shutdown requested")
            switch.shutdown()

            Behaviors.receiveMessagePartial {
              case Stop =>
                logger.info("Stopping because shutdown completed")
                Behaviors.stopped
            }

          case (_, Stop) =>
            logger.info("Stopping because process completed")
            Behaviors.stopped
        }
      }
  }

  def main(args: Array[String]): Unit = {
    implicit val system    = ActorSystem("streamee-demo")
    implicit val mat       = ActorMaterializer()
    implicit val scheduler = system.scheduler

    import system.dispatcher

    val process =
      Flow[(Int, ActorRef[Respondee.Command[String]])]
        .delay(1.second, DelayOverflowStrategy.backpressure)
        .throttle(1, 1.second, 10, ThrottleMode.shaping)
        .map { case (n, p) => ("x" * n, p) }

    val sharding = ClusterSharding(system.toTyped)
    val shutdown = CoordinatedShutdown(system)

    val entityKey: EntityTypeKey[Runner.Command] =
      EntityTypeKey[Runner.Command]("runner")
    sharding.start(ShardedEntity(_ => Runner(process, shutdown), entityKey, Runner.Shutdown))
    Thread.sleep(5000)

    implicit val timeout: Timeout = 1.seconds
    val sinkRefFor: String => Future[SinkRef[(Int, ActorRef[Respondee.Command[String]])]] =
      sharding.entityRefFor(entityKey, _).ask(Runner.GetSinkRef.apply)

    def getIntoableSinkRef(id: String) = {
      println("Getting SinkRef")
      Await.result(sinkRefFor(id), 1.seconds)
    }

    val restartedIntoableSinkRef =
      RestartSink.withBackoff(2.seconds, 4.seconds, 0) { () =>
        getIntoableSinkRef("foo")
      }

    implicit val respondeeFactory: ActorRef[RespondeeFactory.Command[String]] =
      system.spawnAnonymous(RespondeeFactory[String]())

    val done =
      Source(1.to(100))
        .intoRemote(restartedIntoableSinkRef, 10.seconds, 1)
        //.delay(2.seconds, DelayOverflowStrategy.backpressure)
        .toMat(Sink.foreach { s =>
          println(s"client-out: ${s.length}")
        })(Keep.right)
        .run()

    Future.sequence(List(done)).onComplete { result =>
      println(result)
      system.terminate()
    }
  }
}
