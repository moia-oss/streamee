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

import akka.annotation.ApiMayChange
import akka.stream.{ FlowShape, Graph, KillSwitches }
import akka.stream.scaladsl.{
  Broadcast,
  BroadcastHub,
  Flow,
  FlowWithContext,
  GraphDSL,
  Keep,
  Merge,
  MergeHub,
  Sink
}
import scala.concurrent.Future
import scala.util.{ Failure, Success }

package object either {

  /**
    * Extension methods for `FlowWithContext` with output of type `Either`.
    */
  final implicit class EitherFlowWithContextOps[In, CtxIn, Out, CtxOut, Mat, E](
      val flowWithContext: FlowWithContext[In, CtxIn, Either[E, Out], CtxOut, Mat]
  ) extends AnyVal {

    /**
      * Connects this `FlowWithContext` with the given one, thereby only ingesting `Right` elements
      * and passing `Left` elements through.
      *
      * @param viaFlow FlowWithContext` to connect to this one
      * @tparam Out2 output type of the given `FlowWithContext`
      * @return `FlowWithContext` with output of type `Either`
      */
    @ApiMayChange
    def mapVia[Out2](
        viaFlow: Graph[FlowShape[(Out, CtxOut), (Out2, CtxOut)], Any]
    ): FlowWithContext[In, CtxIn, Either[E, Out2], CtxOut, Mat] = {
      val flow =
        Flow.fromGraph(GraphDSL.create(flowWithContext) { implicit builder => flowWithContext =>
          import GraphDSL.Implicits._

          val bcast     = builder.add(Broadcast[(Either[E, Out], CtxOut)](2, eagerCancel = true))
          val merge     = builder.add(Merge[(Either[E, Out2], CtxOut)](2, eagerComplete = true))
          val leftOnly  = FlowWithContext[Either[E, Out], CtxOut].collect { case Left(e) => Left(e) }
          val rightOnly = FlowWithContext[Either[E, Out], CtxOut].collect { case Right(out) => out }
          val mapRight  = FlowWithContext[Out2, CtxOut].map(Right(_))

          // format: OFF
          flowWithContext ~> bcast ~> leftOnly             ~>             merge
                             bcast ~> rightOnly ~> viaFlow ~> mapRight ~> merge
          // format: ON

          FlowShape(flowWithContext.in, merge.out)
        })
      FlowWithContext.fromTuples(flow)
    }

    /**
      * Connects this `FlowWithContext` with the given one, thereby only ingesting `Right` elements
      * and passing `Left` elements through.
      *
      * @param viaFlow FlowWithContext` to connect to this one
      * @tparam Out2 output type of the given `FlowWithContext`
      * @return `FlowWithContext` with output of type `Either`
      */
    @ApiMayChange
    def flatMapVia[Out2](
        viaFlow: Graph[FlowShape[(Out, CtxOut), (Either[E, Out2], CtxOut)], Any]
    ): FlowWithContext[In, CtxIn, Either[E, Out2], CtxOut, Mat] = {
      val flow =
        Flow.fromGraph(GraphDSL.create(flowWithContext) { implicit builder => flowWithContext =>
          import GraphDSL.Implicits._

          val bcast     = builder.add(Broadcast[(Either[E, Out], CtxOut)](2, eagerCancel = true))
          val merge     = builder.add(Merge[(Either[E, Out2], CtxOut)](2, eagerComplete = true))
          val leftOnly  = FlowWithContext[Either[E, Out], CtxOut].collect { case Left(e) => Left(e) }
          val rightOnly = FlowWithContext[Either[E, Out], CtxOut].collect { case Right(out) => out }

          // format: OFF
          flowWithContext ~> bcast ~> leftOnly       ~>       merge
                             bcast ~> rightOnly ~> viaFlow ~> merge
          // format: ON

          FlowShape(flowWithContext.in, merge.out)
        })
      FlowWithContext.fromTuples(flow)
    }

    /**
      * Tap errors (contents of `Left` elements) into the given `Sink` and contents of `Right`
      * elements.
      *
      * @param errorTap `Sink` for errors
      * @return `FlowWithContext` collecting only contents of `Right` elements
      */
    @ApiMayChange
    def errorTo(errorTap: Sink[(E, CtxOut), Any]): FlowWithContext[In, CtxIn, Out, CtxOut, Mat] =
      flowWithContext
        .via(
          Flow.apply.alsoTo(
            Flow[(Either[E, Out], CtxOut)]
              .collect { case (Left(e), ctxOut) => (e, ctxOut) }
              .to(errorTap)
          )
        )
        .collect { case Right(out) => out }
  }

  /**
    * Create a `FlowWithContext` by providing an error `Sink` such that it can be used with the
    * extension method [[EitherFlowWithContextOps.errorTo]].
    *
    * @param f factory for a `FlowWithContext`
    * @tparam In input type of the `FlowWithContext` to be created
    * @tparam Out output type of the `FlowWithContext` to be created
    * @tparam E error type (`Left`) of the `FlowWithContext` to be created
    * @return `FlowWithContext` potentially using the provided error `Sink`
    */
  @ApiMayChange
  def tapErrors[In, CtxIn, Out, CtxOut, Mat, E](
      f: Sink[(E, CtxOut), Any] => FlowWithContext[In, CtxIn, Out, CtxOut, Mat]
  ): FlowWithContext[In, CtxIn, Either[E, Out], CtxOut, Future[Mat]] = {
    val flow =
      Flow.fromMaterializer { case (mat, _) =>
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
}
