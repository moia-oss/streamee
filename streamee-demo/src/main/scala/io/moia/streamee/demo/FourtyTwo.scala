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
import akka.actor.Scheduler
import akka.pattern.after
import akka.stream.scaladsl.{ Flow, GraphDSL, Merge, Unzip }
import akka.stream.FlowShape
import org.apache.logging.log4j.scala.Logging
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.DurationInt
import scala.util.{ Failure, Random, Success }

object FourtyTwo extends Logging {

  type Process = Flow[Request, ErrorOr[Response], NotUsed]

  type Stage[A, B] = Flow[ErrorOr[A], ErrorOr[B], NotUsed]

  type ErrorOr[A] = Either[Error, A]

  sealed trait Error

  final object Error {
    final case object EmptyQuestion                        extends Error
    final case class LookupAnswerFailure(cause: Throwable) extends Error
  }

  // Overall process

  final case class Request(question: String)
  final case class Response(answer: String)

  /**
    * Simple domain logic process for demo purposes. Always answers with "42" ;-)
    */
  def apply()(implicit ec: ExecutionContext, scheduler: Scheduler): Process =
    Flow[Request]
    // Lift into ErrorOr to make all stages look alike
      .map(Right[Error, Request])
      // Via fist stage
      .map(_.map {
        case Request(question) => ValidateQuestionIn(question)
      })
      .via(validateQuestion)
      // Via second stage
      .map(_.map {
        case ValidateQuestionOut(question) => LookupAnswersIn(question)
      })
      .via(lookupAnswersStage)
      // Via third stage
      .map(_.map {
        case LookupAnswersOut(answer) => FourtyTwoIn(answer)
      })
      .via(fourtyTwo)
      // To response
      .map(_.map {
        case FourtyTwoOut(fourtyTwo) => Response(fourtyTwo)
      })

  // First stage

  final case class ValidateQuestionIn(question: String)
  final case class ValidateQuestionOut(question: String)

  def validateQuestion: Stage[ValidateQuestionIn, ValidateQuestionOut] =
    Flow[ErrorOr[ValidateQuestionIn]]
      .map(_.flatMap {
        case ValidateQuestionIn(question) =>
          if (question.isEmpty)
            Left(Error.EmptyQuestion)
          else
            Right(ValidateQuestionOut(question))
      })

  // Second stage

  final case class LookupAnswersIn(question: String)
  final case class LookupAnswersOut(answer: String)

  def lookupAnswersStage(implicit ec: ExecutionContext,
                         scheduler: Scheduler): Stage[LookupAnswersIn, LookupAnswersOut] =
    Flow[ErrorOr[LookupAnswersIn]]
      .mapAsyncUnordered(2) {
        case Left(error) =>
          Future.successful(Left(error))
        case Right(LookupAnswersIn(question)) =>
          lookupAnswer(question)
            .map(answer => LookupAnswersOut(answer))
            .recoverToEither(t => Error.LookupAnswerFailure(t))
      }

  def lookupAnswer(question: String)(implicit ec: ExecutionContext,
                                     scheduler: Scheduler): Future[String] =
    after(4.seconds, scheduler)(Future.successful(question.reverse))

  // Third stage, using a non-trivial graph and sometimes throwing random exceptions

  final case class FourtyTwoIn(answer: String)
  final case class FourtyTwoOut(fourtyTwo: String)

  def fourtyTwo: Stage[FourtyTwoIn, FourtyTwoOut] =
    Flow.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val fromErrorOr =
        builder.add(
          Flow[ErrorOr[FourtyTwoIn]]
            .map {
              case Left(error)                => (Some(error), None)
              case Right(FourtyTwoIn(answer)) => (None, Some(answer))
            }
        )
      val unzip = builder.add(Unzip[Option[Error], Option[String]])
      val failFast =
        builder.add(Flow[Option[Error]].collect {
          case e @ Some(_) => (e, Option.empty[String])
        })
      val collectSuccess = builder.add(Flow[Option[String]].collect { case Some(s) => s })
      val fourtyTwo =
        builder.add(Flow[String].map { _ =>
          if (Random.nextInt(7) == 0) throw new Exception("Random exception again!") else "42"
        })
      val lift  = builder.add(Flow[String].map(s => (Option.empty[Error], Some(s))))
      val merge = builder.add(Merge[(Option[Error], Option[String])](2, eagerComplete = true))
      val toErrorOr =
        builder.add(Flow[(Option[Error], Option[String])].collect {
          case (Some(error), None) => Left(error)
          case (None, Some(s))     => Right(FourtyTwoOut(s))
        })

      // format: off
      fromErrorOr ~> unzip.in
                     unzip.out0 ~> failFast                    ~>         merge.in(0)
                     unzip.out1 ~> collectSuccess ~> fourtyTwo ~> lift ~> merge.in(1)
                                                                          merge.out   ~> toErrorOr
      // format: on

      FlowShape(fromErrorOr.in, toErrorOr.out)
    })

  // Helpers

  /**
    * Extension methods for `Future`s.
    */
  implicit final class FutureOps[A](val fa: Future[A]) extends AnyVal {

    /**
      * Converts any `Future` into a successful one which wraps the value in an `Either`. Hence
      * a successful `Future[A]` becomes a successful `Future[Right[B, A]]` and a failed `Future[A]`
      * becomes a successful `Future[Left[B, A]]`.
      *
      * The use case is `FlowOps.mapAsync` which otherwise would fail the stream in case of a failed
      * `Future`.
      */
    def recoverToEither[B](
        toError: Throwable => B
    )(implicit ec: ExecutionContext): Future[Either[B, A]] =
      fa.transform {
        case Failure(cause) => Success(Left(toError(cause)))
        case Success(a)     => Success(Right(a))
      }
  }
}
