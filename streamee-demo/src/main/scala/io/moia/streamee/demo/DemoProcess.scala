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

package io.moia.streamee.demo

import akka.NotUsed
import akka.actor.Scheduler
import akka.pattern.after
import akka.stream.scaladsl.Flow
import io.moia.streamee.demo.DemoProcess.Error.LookupAnswerFailure
import java.util.UUID
import org.apache.logging.log4j.scala.Logging
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.DurationInt
import scala.util.{ Failure, Success }

object DemoProcess extends Logging {

  type Process = Flow[Request, ErrorOr[Response], NotUsed]

  type Stage[A, B] = Flow[ErrorOr[A], ErrorOr[B], NotUsed]

  type ErrorOr[A] = Either[Error, A]

  sealed trait Error {
    def correlationId: UUID
  }

  final object Error {
    final case class EmptyQuestion(correlationId: UUID)                         extends Error
    final case class LookupAnswerFailure(cause: Throwable, correlationId: UUID) extends Error
  }

  // Overall process

  final case class Request(question: String, correlationId: UUID = UUID.randomUUID())
  final case class Response(answer: String, correlationId: UUID = UUID.randomUUID())

  /**
    * Simple domain logic process for demo purposes. Always answers with "42" ;-)
    */
  def apply()(implicit ec: ExecutionContext, scheduler: Scheduler): Process =
    Flow[Request]
      .map(Right[Error, Request]) // To make all stages look alike
      // fist stage
      .map(_.map {
        case request @ Request(question, _) => ValidateQuestionIn(question, request)
      })
      .via(validateQuestion)
      // second stage
      .map(_.map {
        case ValidateQuestionOut(question, request) => LookupAnswerIn(question, request)
      })
      .via(lookupAnswerStage)
      // third stage
      .map(_.map {
        case LookupAnswerOut(_, request) => FourtyTwoIn(request)
      })
      .via(fourtyTwo)
      // response
      .map(_.map {
        case FourtyTwoOut(fourtyTwo, request) => Response(fourtyTwo, request.correlationId)
      })

  // First stage

  final case class ValidateQuestionIn(question: String, request: Request)
  final case class ValidateQuestionOut(question: String, request: Request)

  def validateQuestion: Stage[ValidateQuestionIn, ValidateQuestionOut] =
    Flow[ErrorOr[ValidateQuestionIn]]
      .map(_.flatMap {
        case ValidateQuestionIn(question, request) =>
          if (question.isEmpty)
            Left(Error.EmptyQuestion(request.correlationId))
          else
            Right(ValidateQuestionOut(question, request))
      })

  // Second stage

  final case class LookupAnswerIn(question: String, request: Request)
  final case class LookupAnswerOut(answer: String, request: Request)

  def lookupAnswerStage(implicit ec: ExecutionContext,
                        scheduler: Scheduler): Stage[LookupAnswerIn, LookupAnswerOut] =
    Flow[ErrorOr[LookupAnswerIn]]
      .mapAsyncUnordered(2) {
        case Left(error) =>
          Future.successful(Left(error))
        case Right(LookupAnswerIn(question, request)) =>
          lookupAnswer(question)
            .map(answer => LookupAnswerOut(answer, request))
            .recoverToEither(t => LookupAnswerFailure(t, request.correlationId))
      }

  def lookupAnswer(question: String)(implicit ec: ExecutionContext,
                                     scheduler: Scheduler): Future[String] =
    after(4.seconds, scheduler)(Future.successful(question.reverse))

  // Third stage

  final case class FourtyTwoIn(request: Request)
  final case class FourtyTwoOut(fourtyTwo: String, request: Request)

  def fourtyTwo: Stage[FourtyTwoIn, FourtyTwoOut] =
    Flow[ErrorOr[FourtyTwoIn]]
      .map(_.map {
        case FourtyTwoIn(request) => FourtyTwoOut("42", request)
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
