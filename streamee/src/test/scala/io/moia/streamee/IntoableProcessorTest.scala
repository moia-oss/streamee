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

import akka.Done
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter.UntypedActorSystemOps
import akka.actor.typed.{ ActorRef, ActorSystem, Behavior }
import akka.stream.Materializer
import akka.stream.typed.scaladsl.ActorMaterializer
import org.apache.logging.log4j.scala.Logging
import org.scalatest.WordSpec
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ ExecutionContext, Future }

class IntoableProcessorTest extends WordSpec with AkkaSuite {

  implicit val typedSystem: ActorSystem[Nothing] = system.toTyped

  "IntoableProcessor" should {
    "wait for the 1 operation to finish " in { ingest(1) }
    "wait for the 100 operations to finish " in { ingest(100) }
  }

  def ingest(numElements: Int): Unit = {
    val flowsActor = system.spawnAnonymous(FlowsOnState())
    // send elements to process
    val probes = 0.until(numElements).map { n =>
      val probe = TestProbe[String]
      flowsActor ! FlowsOnState.ProcessElement(n, probe.ref)
      probe
    }
    // shutdown the actor
    flowsActor ! FlowsOnState.Shutdown
    // expect all messages to be processed
    probes.map(_.expectMessageType[String](5.seconds))
  }
}

// This actor starts and uses flows to process elements.
object FlowsOnState extends Logging {

  sealed trait Command
  final case object Shutdown                                            extends Command
  final case class ProcessElement(int: Int, actorRef: ActorRef[String]) extends Command
  private final case object Stop                                        extends Command

  def apply(): Behavior[Command] = Behaviors.setup { context =>
    implicit val materializer: Materializer = ActorMaterializer.boundToActor(context)
    implicit val ec: ExecutionContext       = context.executionContext
    val onShutdown                          = ListBuffer.empty[() => Future[_]]

    // this method is used to register a hook that should be called during teardown as
    // well as a future that succeeds when the process dies
    def registerShutdownHook[T](process: String,
                                triggerShutdown: => Unit,
                                future: Future[T]): Unit = {
      // register callback: if this future completes we need to shutdown
      future.onComplete { reason =>
        logger.warn(s"Process completed $process with reason $reason")
        context.self ! Shutdown
      }
      // if a shutdown is triggered we need to stop this process
      onShutdown += { () =>
        logger.info(s"Tear down $process")
        triggerShutdown
        future
      }
    }

    // we tear down all started flows in reverse order and wait for complete for each process
    def tearDown(): Future[_] =
      onShutdown.reverse.foldLeft[Future[_]](Future.successful(Done))(
        (whenDone, hook) => whenDone.flatMap(_ => hook.apply())
      )

    // intoable processor that could be used in different other flows
    val toStringProcessor =
      IntoableProcessor(Process[Int, String]().delay(100.milli).map(int => s"Got $int"),
                        "wait-to-string",
                        100)
    registerShutdownHook("toStringProcessor",
                         toStringProcessor.shutdown(),
                         toStringProcessor.whenDone)

    // front processor that is used to kickoff a process
    val doubleToStringProcessor = FrontProcessor(
      Process[Int, String]().delay(100.millis).map(_ * 2).into(toStringProcessor.sink, 3.seconds),
      1.second,
      "wait-and-double",
      100
    )
    registerShutdownHook("doubleToString",
                         doubleToStringProcessor.shutdown(),
                         doubleToStringProcessor.whenDone)

    Behaviors
      .receiveMessagePartial[Command] {
        case Shutdown =>
          // teardown all started flows. Once all have stopped, stop the actor.
          tearDown().onComplete(_ => context.self ! Stop)
          Behaviors.receiveMessagePartial {
            case Shutdown => Behaviors.same
            case Stop     => Behaviors.stopped
          }
        case ProcessElement(element, ref) =>
          // process an element
          doubleToStringProcessor.accept(element).map(ref ! _)
          Behaviors.same
      }
  }
}
