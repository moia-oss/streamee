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

import akka.{ Done, NotUsed }
import akka.actor.{ CoordinatedShutdown, Scheduler }
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter.TypedActorSystemOps
import akka.http.scaladsl.model.StatusCodes.ServiceUnavailable
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.ExceptionHandler
import akka.stream.{
  Attributes,
  ClosedShape,
  FanInShape2,
  Inlet,
  Materializer,
  Outlet,
  OverflowStrategy,
  QueueOfferResult
}
import akka.stream.scaladsl.{
  Flow,
  GraphDSL,
  Keep,
  RunnableGraph,
  Sink,
  Source,
  SourceQueueWithComplete,
  Unzip
}
import akka.stream.stage.{
  GraphStage,
  GraphStageLogic,
  InHandler,
  OutHandler,
  TimerGraphStageLogic
}
import akka.stream.QueueOfferResult.{ Dropped, Enqueued }
import io.moia.streamee.Processor.{ ProcessorUnavailable, UnexpectedQueueOfferResult }
import org.apache.logging.log4j.scala.Logging
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration.FiniteDuration

/**
  * Runs a domain logic process (Akka Streams flow) accepting requests and producing responses. See
  * [[Processor.apply]] for details.
  */
object Processor {

  final case class ProcessorUnavailable(name: String)
      extends Exception(s"Processor $name cannot accept requests at this time!")

  final case class UnexpectedQueueOfferResult(result: QueueOfferResult)
      extends Exception(s"QueueOfferResult $result was not expected!")

  implicit val processorUnavailableHandler: ExceptionHandler =
    ExceptionHandler {
      case ProcessorUnavailable(name) =>
        complete(ServiceUnavailable -> s"Processor $name cannot accept offers at this time!")
    }

  /**
    * Runs a domain logic process (Akka Streams flow) accepting requests and producing responses.
    * Requests offered via the returned queue are pushed into the given `process`. Once responses
    * are available, the promise given together with the request is completed with success. If the
    * process back-pressures, offered requests are dropped (fail fast).
    *
    * This factory is conveniently creating default [[ProcessorSettings]] and also registering
    * the returned [[Processor]] with coordinated shutdown.
    *
    * @param process domain logic process from request to response
    * @param name name, used e.g. in [[ProcessorUnavailable]] exceptions
    * @param correlateRequest correlation function for the request
    * @param correlateResponse correlation function for the response
    * @tparam A request type
    * @tparam B response type
    * @return [[Processor]] for offering requests and shutting down, already registered with coordinated shutdown
    */
  def apply[A, B, C](process: Flow[A, B, Any], name: String)(
      correlateRequest: A => C,
      correlateResponse: B => C
  )(implicit mat: Materializer, ec: ExecutionContext, system: ActorSystem[_]): Processor[A, B] =
    Processor(process, name, ProcessorSettings(system))(correlateRequest, correlateResponse)(
      mat,
      ec,
      system.scheduler
    ).registerWithCoordinatedShutdown(CoordinatedShutdown(system.toUntyped))

  /**
    * Runs a domain logic process (Akka Streams flow) accepting requests and producing responses.
    * Requests offered via the returned queue are pushed into the given `process`. Once responses
    * are available, the promise given together with the request is completed with success. If the
    * process back-pressures, offered requests are dropped (fail fast).
    *
    * @param process domain logic process from request to response
    * @param name name, used e.g. in [[ProcessorUnavailable]] exceptions
    * @param settings settings for processors (from application.conf or reference.conf)
    * @param correlateRequest correlation function for the request
    * @param correlateResponse correlation function for the response
    * @tparam A request type
    * @tparam B response type
    * @return [[Processor]] for offering requests and shutting down
    */
  def apply[A, B, C](process: Flow[A, B, Any], name: String, settings: ProcessorSettings)(
      correlateRequest: A => C,
      correlateResponse: B => C
  )(implicit mat: Materializer, ec: ExecutionContext, scheduler: Scheduler): Processor[A, B] = {
    import settings._

    val source =
      Source
        .queue[(A, Promise[B])](bufferSize, OverflowStrategy.dropNew) // Must be 1: for 0 offers could "hang", for larger values completing would not work, see: https://github.com/akka/akka/issues/25349
        .map { case reqAndRes @ (req, _) => (reqAndRes, req) }

    val (queue, done) =
      RunnableGraph
        .fromGraph(GraphDSL.create(source, Sink.ignore)(Keep.both) {
          implicit builder => (source, ignore) =>
            import GraphDSL.Implicits._

            val unzip = builder.add(Unzip[(A, Promise[B]), A]())
            val promises =
              builder.add(
                new PromisesStage[A, B, C](correlateRequest,
                                           correlateResponse,
                                           sweepCompleteResponsesInterval)
              )

            // format: off
            source ~> unzip.in
                      unzip.out0       ~>      promises.in0
                      unzip.out1 ~> process ~> promises.in1
                                               promises.out ~> ignore
            // format: on

            ClosedShape
        })
        .run()

    new ProcessorImpl(queue, done, name)
  }
}

/**
  * Process requests with a running process or shut it down.
  */
sealed trait Processor[A, B] {

  /**
    * Offers the given request to the running process. The returned `Future` is either completed
    * successfully with the response or failed if it cannot be offered, e.g. because of back
    * pressure.
    *
    * @param request request to be processed
    * @param timeout maximum duration for the request to be processed, i.e. the related promise to
    *                be completed
    * @return `Future` for the response
    */
  def process(request: A, timeout: FiniteDuration)(implicit ec: ExecutionContext,
                                                   scheduler: Scheduler): Future[B]

  /**
    * Shuts down the running process. Already accepted requests are still processed, but no new ones
    * accepted. The returened `Future` is completed once all requests have been processed.
    *
    * @return `Future` signaling that all requests have been processed
    */
  def shutdown(): Future[Done]

  /**
    * Registers shutdown of this processor during coordinated shutdown in the service-requests-done
    * phase.
    *
    * @return this instance
    */
  def registerWithCoordinatedShutdown(coordinatedShutdown: CoordinatedShutdown): this.type = {
    coordinatedShutdown.addTask(CoordinatedShutdown.PhaseServiceRequestsDone, "processor") { () =>
      shutdown()
    }
    this
  }
}

private final class ProcessorImpl[A, B](queue: SourceQueueWithComplete[(A, Promise[B])],
                                        done: Future[Done],
                                        name: String)
    extends Processor[A, B] {

  override def process(request: A, timeout: FiniteDuration)(implicit ec: ExecutionContext,
                                                            scheduler: Scheduler): Future[B] = {
    val promisedResponse = ExpiringPromise[B](timeout, request.toString)
    queue.offer((request, promisedResponse)).flatMap {
      case Enqueued => promisedResponse.future
      case Dropped  => Future.failed(ProcessorUnavailable(name))
      case other    => Future.failed(UnexpectedQueueOfferResult(other))
    }
  }

  override def shutdown(): Future[Done] = {
    queue.complete()
    done
  }
}

private final class PromisesStage[A, B, C](correlateRequest: A => C,
                                           correlateResponse: B => C,
                                           gcInterval: FiniteDuration)
    extends GraphStage[FanInShape2[(A, Promise[B]), B, NotUsed]]
    with Logging {

  override val shape: FanInShape2[(A, Promise[B]), B, NotUsed] =
    new FanInShape2(Inlet[(A, Promise[B])]("PromisesStage.in0"),
                    Inlet[B]("PromisesStage.in1"),
                    Outlet[NotUsed]("PromisesStage.out"))

  override def createLogic(attributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) {
      import shape._

      private var reqToRes = Map.empty[C, Promise[B]]

      setHandler(
        in0,
        new InHandler {
          override def onPush(): Unit = {
            val (req, res) = grab(in0)
            reqToRes += correlateRequest(req) -> res
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
            reqToRes.get(correlateResponse(res)).foreach(_.trySuccess(res))
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
        schedulePeriodicallyWithInitialDelay("gc", gcInterval, gcInterval)

      override protected def onTimer(timerKey: Any): Unit =
        reqToRes = reqToRes.filter { case (_, res) => !res.isCompleted }
    }
}
