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
package processor

import akka.actor.Scheduler
import akka.stream.QueueOfferResult.{ Dropped, Enqueued }
import akka.stream.scaladsl.{ Flow, GraphDSL, Keep, RunnableGraph, Sink, Source, Unzip }
import akka.stream.stage.{
  GraphStage,
  GraphStageLogic,
  InHandler,
  OutHandler,
  TimerGraphStageLogic
}
import akka.stream.{
  ActorAttributes,
  Attributes,
  ClosedShape,
  FanInShape2,
  Inlet,
  Materializer,
  Outlet,
  OverflowStrategy,
  Supervision
}
import akka.{ Done, NotUsed }
import io.moia.streamee.processor.Processor.{ ProcessorError, ProcessorUnavailable }
import org.apache.logging.log4j.scala.Logging
import scala.concurrent.duration.{ Duration, FiniteDuration }
import scala.concurrent.{ ExecutionContext, Future, Promise }

private object PermanentProcessor extends Logging {

  final class PromisesStage[A, B, C](correlateRequest: A => C,
                                     correlateResponse: B => C,
                                     sweepCompleteResponsesInterval: FiniteDuration)
      extends GraphStage[FanInShape2[(A, Promise[B]), B, NotUsed]]
      with Logging {

    override val shape: FanInShape2[(A, Promise[B]), B, NotUsed] =
      new FanInShape2(Inlet[(A, Promise[B])]("PromisesStage.in0"),
                      Inlet[B]("PromisesStage.in1"),
                      Outlet[NotUsed]("PromisesStage.out"))

    override def createLogic(attributes: Attributes): GraphStageLogic =
      new TimerGraphStageLogic(shape) {
        import shape._

        private var responses = Map.empty[C, Promise[B]]

        setHandler(
          in0,
          new InHandler {
            override def onPush(): Unit = {
              val (req, res) = grab(in0)
              responses += correlateRequest(req) -> res
              if (!hasBeenPulled(in0)) pull(in0)
            }

            // Only in1 must complete the stage!
            override def onUpstreamFinish(): Unit =
              ()
          }
        )

        setHandler(
          in1,
          new InHandler {
            override def onPush(): Unit = {
              val res = grab(in1)
              responses.get(correlateResponse(res)).foreach(_.trySuccess(res))
              push(out, NotUsed)
            }
          }
        )

        setHandler(out, new OutHandler {
          override def onPull(): Unit = {
            if (!isClosed(in0) && !hasBeenPulled(in0)) pull(in0)
            pull(in1)
          }
        })

        override def preStart(): Unit =
          schedulePeriodicallyWithInitialDelay("gc",
                                               sweepCompleteResponsesInterval,
                                               sweepCompleteResponsesInterval)

        override protected def onTimer(timerKey: Any): Unit =
          responses = responses.filter { case (_, response) => !response.isCompleted }
      }
  }

  def resume(name: String)(cause: Throwable): Supervision.Directive = {
    logger.error(s"Processor $name failed and resumes", cause)
    Supervision.Resume
  }
}

private final class PermanentProcessor[A, B, C](
    process: Flow[A, B, Any],
    timeout: FiniteDuration,
    name: String,
    bufferSize: Int,
    correlateRequest: A => C,
    correlateResponse: B => C
)(implicit ec: ExecutionContext, mat: Materializer, scheduler: Scheduler)
    extends Processor[A, B] {
  import PermanentProcessor._

  require(timeout > Duration.Zero, s"timeout must be positive, but was $timeout!")
  require(bufferSize >= 0, s"bufferSize must not be negative, but was $bufferSize!")

  private val source =
    Source
      .queue[(A, Promise[B])](bufferSize, OverflowStrategy.dropNew)
      .map { case requestAndResponse @ (request, _) => (requestAndResponse, request) }

  private val (queue, done) =
    RunnableGraph
      .fromGraph(GraphDSL.create(source, Sink.ignore)(Keep.both) {
        implicit builder => (source, ignore) =>
          import GraphDSL.Implicits._

          val unzip = builder.add(Unzip[(A, Promise[B]), A]())
          val promises =
            builder.add(
              new PromisesStage[A, B, C](correlateRequest, correlateResponse, timeout * 2)
            )

          // format: off
          source ~> unzip.in
                    unzip.out0  ~>            promises.in0
                    unzip.out1  ~> process ~> promises.in1
                                              promises.out ~> ignore
          // format: on

          ClosedShape
      })
      .withAttributes(ActorAttributes.supervisionStrategy(resume(name)))
      .run()

  override def process(request: A): Future[B] = {
    val response = ExpiringPromise[B](timeout, s"from processor $name for request $request")
    queue.offer((request, response)).flatMap {
      case Enqueued => response.future
      case Dropped  => Future.failed(ProcessorUnavailable(name))
      case other    => Future.failed(ProcessorError(other))
    }
  }

  override def shutdown(): Future[Done] = {
    queue.complete()
    done
  }
}
