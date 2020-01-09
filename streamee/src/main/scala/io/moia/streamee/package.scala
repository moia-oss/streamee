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

package io.moia

import akka.actor.typed.ActorRef
import akka.actor.CoordinatedShutdown
import akka.annotation.ApiMayChange
import akka.stream.{ DelayOverflowStrategy, KillSwitches, Materializer, SinkRef, ThrottleMode }
import akka.stream.scaladsl.{
  BroadcastHub,
  Flow,
  FlowOps,
  FlowWithContext,
  Keep,
  MergeHub,
  Sink,
  Source
}
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration.{ Duration, FiniteDuration }
import scala.util.{ Failure, Success }

package object streamee {

  /**
    * A domain logic process from a request to a response which transparently propagates a
    * [[Respondee]] for the response. Can be used locally or remotely because the propagated
    * [[Respondee]] is location transparent.
    *
    * Use [[Process]] to create an empty initial process [[Step]], i.e. one where the context
    * is a [[Respondee]].
    */
  type Process[-Req, Res] = Step[Req, Res, Respondee[Res]]

  /**
    * A step within a [[Process]]. The context is not fixed to be a [[Respondee]], although in
    * order to compose [[Step]]s into a [[Process]] a [[Respondee]] must at least be a part of
    * the context.
    *
    * Use [[Step]] to create an empty initial [[Step]].
    */
  type Step[-In, +Out, Ctx] = FlowWithContext[In, Ctx, Out, Ctx, Any]

  /**
    * Convenient shortcut for `ActorRef[Respondee.Response[A]]`.
    */
  type Respondee[A] = ActorRef[Respondee.Response[A]]

  /**
    * Convenient shortcut for `Sink[(In, Respondee[Out]), Any]`.
    */
  type ProcessSink[-In, Out] = Sink[(In, Respondee[Out]), Any]

  /**
    * Convenient shortcut for `SinkRef[(In, Respondee[Out])]`.
    */
  type ProcessSinkRef[In, Out] = SinkRef[(In, Respondee[Out])]

  /**
    * Signals no response within the given timeout.
    *
    * @param timeout maximum duration for the response
    */
  final case class ResponseTimeoutException(timeout: FiniteDuration)
      extends Exception(s"No response within $timeout!")

  /**
    * Extension methods for `Source`.
    */
  final implicit class SourceExt[Out, Mat](val source: Source[Out, Mat]) extends AnyVal {

    /**
      * Ingest into the given [[ProcessSink]] and emit its response or fail with
      * [[ResponseTimeoutException]], if the response is not produced in time.
      *
      * @param processSink [[ProcessSink]] to emit into
      * @param timeout maximum duration for the running process to respond; must be positive!
      * @param parallelism maximum duration for the running process to respond; must be positive!
      * @tparam Out2 output type of the given [[ProcessSink]]
      * @return `Source` emitting responses of the given [[ProcessSink]]
      */
    def into[Out2](
        processSink: ProcessSink[Out, Out2],
        timeout: FiniteDuration,
        parallelism: Int
    ): Source[Out2, Future[Mat]] = {
      require(timeout > Duration.Zero, s"timeout must be > 0, but was $timeout!")
      require(parallelism > 0, s"parallelism must be > 0, but was $parallelism!")

      Source.fromMaterializer((mat, _) => intoImpl(source, processSink, timeout, mat, parallelism))
    }
  }

  /**
    * Extension methods for `Flow`.
    */
  final implicit class FlowExt[In, Out, Mat](val flow: Flow[In, Out, Mat]) extends AnyVal {

    /**
      * Ingest into the given [[ProcessSink]] and emit its response or fail with
      * [[ResponseTimeoutException]], if the response is not produced in time.
      *
      * @param processSink [[ProcessSink]] to emit into
      * @param timeout maximum duration for the running process to respond; must be positive!
      * @param parallelism maximum duration for the running process to respond; must be positive!
      * @tparam Out2 output type of the given [[ProcessSink]]
      * @return `Source` emitting responses of the given [[ProcessSink]]
      */
    def into[Out2](
        processSink: ProcessSink[Out, Out2],
        timeout: FiniteDuration,
        parallelism: Int
    ): Flow[In, Out2, Future[Mat]] = {
      require(timeout > Duration.Zero, s"timeout must be > 0, but was $timeout!")
      require(parallelism > 0, s"parallelism must be > 0, but was $parallelism!")

      Flow.fromMaterializer((mat, _) => intoImpl(flow, processSink, timeout, mat, parallelism))
    }
  }

  /**
    * Extension methods for `FlowWithContext`.
    */
  final implicit class FlowWithContextExt[In, CtxIn, Out, CtxOut, Mat](
      val flowWithContext: FlowWithContext[In, CtxIn, Out, CtxOut, Mat]
  ) extends AnyVal {

    /**
      * Ingest into the given [[ProcessSink]] and emit its response or fail with
      * [[ResponseTimeoutException]], if the response is not produced in time.
      *
      * @param processSink [[ProcessSink]] to emit into
      * @param timeout maximum duration for the running process to respond; must be positive!
      * @param parallelism maximum duration for the running process to respond; must be positive!
      * @tparam Out2 output type of the given [[ProcessSink]]
      * @return `FlowWithContext` emitting responses of the given [[ProcessSink]]
      */
    def into[Out2](
        processSink: ProcessSink[Out, Out2],
        timeout: FiniteDuration,
        parallelism: Int
    ): FlowWithContext[In, CtxIn, Out2, CtxOut, Future[Mat]] = {
      require(timeout > Duration.Zero, s"timeout must be > 0, but was $timeout!")
      require(parallelism > 0, s"parallelism must be > 0, but was $parallelism!")

      FlowWithContext.fromTuples(Flow.fromMaterializer { (mat, _) =>
        flowWithContext
          .map(spawnRespondee[Out, Out2](timeout, mat))
          .via(Flow.apply.alsoTo {
            Flow[((Out, Respondee[Out2], Promise[Out2]), CtxOut)]
              .map { case ((out, respondee2, _), _) => (out, respondee2) }
              .to(processSink)
          })
          .mapAsync(parallelism)(_._3.future)
          .asFlow
      })
    }

    /**
      * Push the emitted element transformed by the given function `f` to the propagated context and
      * also transform the emitted element by the given function `g`.
      *
      * @param f transform the emitted element before pushing to the context
      * @param g transform the emitted element
      * @tparam A target type of the transformation of the element pushed to the context
      * @tparam B target type of the transformation of the element
      * @return `FlowWithContext` propagating its transformed input elements along with the context
      *         and emitting transformed input elements
      */
    def push[A, B](f: Out => A, g: Out => B): FlowWithContext[In, CtxIn, B, (A, CtxOut), Any] =
      flowWithContext.via(Flow.apply.map { case (out, ctxOut) => (g(out), (f(out), ctxOut)) })

    /**
      * Push the emitted element to the propagated context.
      */
    def push: FlowWithContext[In, CtxIn, Out, (Out, CtxOut), Any] =
      push(identity, identity)
  }

  /**
    * Extension methods for `FlowWithContext` with paired output context; see
    * [[FlowWithContextExt]].
    */
  final implicit class FlowWithPairedContextOps[In, CtxIn, Out, CtxOut, Mat, A](
      val flowWithContext: FlowWithContext[In, CtxIn, Out, (A, CtxOut), Mat]
  ) extends AnyVal {

    /**
      * Pop a formerly pushed and potentially transformed element from the propagated context and
      * pair it up with the emitted element.
      *
      * @return `FlowWithContext` propagating the former context only and emitting the propagated
      *         transformed former input elements along with its actual input elements
      */
    def pop: FlowWithContext[In, CtxIn, (A, Out), CtxOut, Mat] =
      flowWithContext.via(Flow.apply.map { case (out, (a, ctxOut)) => ((a, out), ctxOut) })
  }

  /**
    * Extension methods for `FlowWithContext` with output of type `Either`.
    */
  final implicit class EitherFlowWithContextOps[In, CtxIn, Out, CtxOut, Mat, E](
      val flowWithContext: FlowWithContext[In, CtxIn, Either[E, Out], CtxOut, Mat]
  ) extends AnyVal {

    /**
      * Tap errors (contents of `Left` elements) into the given `Sink` and contents of `Right`
      * elements.
      *
      * @param errors error `Sink`
      * @return `FlowWithContext` collecting only contents of `Right` elements
      */
    @ApiMayChange
    def errorTo(errors: Sink[(E, CtxOut), Any]): FlowWithContext[In, CtxIn, Out, CtxOut, Mat] =
      flowWithContext
        .via(
          Flow.apply.alsoTo(
            Flow[(Either[E, Out], CtxOut)]
              .collect { case (Left(e), ctxOut) => (e, ctxOut) }
              .to(errors)
          )
        )
        .collect { case Right(out) => out }
  }

  /**
    * Extension methods for `ProcessSink`.
    */
  final implicit class ProcessSinkOps[Req, Res](val sink: ProcessSink[Req, Res]) extends AnyVal {

    /**
      * Creates a canonical [[FrontProcessor]] from this [[ProcessSink]].
      *
      * @param timeout maximum duration for the running process to respond; must be positive!
      * @param parallelism maximum duration for the running process to respond; must be positive!
      * @param name name, used for logging and exceptions
      * @param bufferSize optional size of the buffer of the used `Source.queue`; defaults to 1;
      *                   must be positive!
      * @param phase identifier for a phase of `CoordinatedShutdown`; defaults to
      *              "service-requests-done"; must be defined in configufation!
      * @return [[FrontProcessor]]
      */
    def asFrontProcessor(
        timeout: FiniteDuration,
        parallelism: Int,
        name: String,
        bufferSize: Int = 1,
        phase: String = CoordinatedShutdown.PhaseServiceRequestsDone
    )(implicit mat: Materializer, ec: ExecutionContext): FrontProcessor[Req, Res] = {
      require(parallelism > 0, s"parallelism must be > 0, but was $parallelism!")

      FrontProcessor(
        Process[Req, Res].into(sink, timeout, parallelism),
        timeout,
        name,
        bufferSize,
        phase
      )
    }
  }

  /**
    * Missing standard operators from `FlowOps` not yet defined on `FlowWithContext` (by Akka).
    */
  final implicit class FlowWithContextOpsAkka[In, CtxIn, Out, CtxOut](
      val flowWithContext: FlowWithContext[In, CtxIn, Out, CtxOut, Any]
  ) extends AnyVal {

    def delay(
        of: FiniteDuration,
        strategy: DelayOverflowStrategy = DelayOverflowStrategy.dropTail
    ): flowWithContext.Repr[Out, CtxOut] =
      flowWithContext.via(Flow.apply.delay(of, strategy))

    def throttle(
        elements: Int,
        per: FiniteDuration,
        maximumBurst: Int,
        mode: ThrottleMode
    ): flowWithContext.Repr[Out, CtxOut] =
      flowWithContext.via(Flow.apply.throttle(elements, per, maximumBurst, mode))
  }

  /**
    * Wraps the given [[Step]] in one emitting its input together with its output (as a `Tuple2`).
    *
    * Notice that thanks to type inference there should be no special requirements regarding the
    * context type of the given step, i.e. if you define it in a generic way – as a parameterized
    * method like usual – it should work without any type annotations:
    *
    *{{{
    *def length[Ctx]: Step[String, Int, Ctx] =
    *  Step[String, Ctx].map(_.length)
    *
    *val process: Process[String, (String, Int)] =
    *  zipWithIn(lenght) // No need to give type args to length!
    *}}}
    *
    * @param step [[Step]] to be wrapped
    * @tparam In input type of the given [[Step]]
    * @tparam Out output type of the given [[Step]]
    * @tparam Ctx context type of the given [[Step]]
    * @return [[Step]] emitting the input of the wrapped one together with its ouput (as a `Tuple2`)
    */
  def zipWithIn[In, Out, Ctx](step: Step[In, Out, (In, Ctx)]): Step[In, (In, Out), Ctx] =
    Step[In, Ctx].push.via(step).pop

  /**
    * Prepare an error `Sink` for a `FlowWithContext` with output of type `Either` such that it can
    * be used with the extension method [[EitherFlowWithContextOps.errorTo]].
    *
    * @param f factory for a `FlowWithContext` with output of type `Either`
    * @tparam In input type of the `FlowWithContext` to be created
    * @tparam Out output type of the `FlowWithContext` to be created
    * @tparam E error type (`Left`) of the `FlowWithContext` to be created
    * @return `FlowWithContext` with output of type `Either`
    */
  @ApiMayChange
  def tapErrors[In, CtxIn, Out, CtxOut, Mat, E](
      f: Sink[(E, CtxOut), Any] => FlowWithContext[In, CtxIn, Out, CtxOut, Mat]
  ): FlowWithContext[In, CtxIn, Either[E, Out], CtxOut, Future[Mat]] = {
    val flow =
      Flow.fromMaterializer {
        case (mat, _) =>
          val ((errorTap, switch), errors) =
            MergeHub
              .source[(E, CtxOut)](1)
              .viaMat(KillSwitches.single)(Keep.both)
              .toMat(BroadcastHub.sink[(E, CtxOut)])(Keep.both)
              .run()(mat)
          f(errorTap)
            .map(Right.apply)
            .asFlow
            .alsoTo(
              Flow[Any]
                .to(Sink.onComplete {
                  case Success(_)     => switch.shutdown()
                  case Failure(cause) => switch.abort(cause)
                })
            )
            .merge(errors.map { case (e, ctxOut) => (Left(e), ctxOut) })
      }
    FlowWithContext.fromTuples(flow)
  }

  private def intoImpl[Out, Out2, M](
      flowOps: FlowOps[Out, M],
      processSink: ProcessSink[Out, Out2],
      timeout: FiniteDuration,
      mat: Materializer,
      parallelism: Int
  ) =
    flowOps
      .map(spawnRespondee[Out, Out2](timeout, mat))
      .alsoTo {
        Flow[(Out, Respondee[Out2], Promise[Out2])]
          .map { case (out, respondee2, _) => (out, respondee2) }
          .to(processSink)
      }
      .mapAsync(parallelism)(_._3.future)

  private def spawnRespondee[Out, Out2](timeout: FiniteDuration, mat: Materializer)(out: Out) = {
    val (respondee2, out2) = Respondee.spawn[Out2](timeout)(mat)
    (out, respondee2, out2)
  }
}
