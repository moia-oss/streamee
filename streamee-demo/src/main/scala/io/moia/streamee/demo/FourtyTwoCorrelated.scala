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

///*
// * Copyright 2018 MOIA GmbH
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package io.moia.streamee
//package demo
//
//import akka.NotUsed
//import akka.actor.Scheduler
//import akka.pattern.after
//import akka.stream.FlowShape
//import akka.stream.scaladsl.{ Flow, GraphDSL, Merge, Unzip }
//import java.util.UUID
//import org.apache.logging.log4j.scala.Logging
//import scala.concurrent.duration.DurationInt
//import scala.concurrent.{ ExecutionContext, Future }
//import scala.util.{ Failure, Random, Success }
//
//object FourtyTwoCorrelated extends Logging {
//
//  type Process = Flow[Request, ErrorOr[Response], NotUsed]
//
//  type Stage[A, B] = Flow[ErrorOr[A], ErrorOr[B], NotUsed]
//
//  type ErrorOr[A] = Either[Error, A]
//
//  sealed trait Error {
//    def correlationId: UUID
//  }
//
//  final object Error {
//    final case class EmptyQuestion(correlationId: UUID)                         extends Error
//    final case class LookupAnswerFailure(cause: Throwable, correlationId: UUID) extends Error
//  }
//
//  // Overall process
//
//  final case class Request(question: String, correlationId: UUID = UUID.randomUUID())
//  final case class Response(answer: String, correlationId: UUID = UUID.randomUUID())
//
//  /**
//    * Simple domain logic process for demo purposes. Always answers with "42" ;-)
//    */
//  def apply()(implicit ec: ExecutionContext, scheduler: Scheduler): Process =
//    Flow[Request]
//    // Lift into ErrorOr to make all stages look alike
//      .map(Right[Error, Request])
//      // Via fist stage
//      .map(_.map {
//        case Request(question, correlationId) => ValidateQuestionIn(question, correlationId)
//      })
//      .via(validateQuestion)
//      // Via second stage
//      .map(_.map {
//        case ValidateQuestionOut(question, correlationId) =>
//          LookupAnswersIn(question, correlationId)
//      })
//      .via(lookupAnswersStage)
//      // Via third stage
//      .map(_.map {
//        case LookupAnswersOut(answer, correlationId) => FourtyTwoIn(answer, correlationId)
//      })
//      .via(fourtyTwo)
//      // To response
//      .map(_.map {
//        case FourtyTwoOut(fourtyTwo, correlationId) => Response(fourtyTwo, correlationId)
//      })
//
//  // First stage
//
//  final case class ValidateQuestionIn(question: String, correlationId: UUID)
//  final case class ValidateQuestionOut(question: String, correlationId: UUID)
//
//  def validateQuestion: Stage[ValidateQuestionIn, ValidateQuestionOut] =
//    Flow[ErrorOr[ValidateQuestionIn]]
//      .map(_.flatMap {
//        case ValidateQuestionIn(question, correlationId) =>
//          if (question.isEmpty)
//            Left(Error.EmptyQuestion(correlationId))
//          else
//            Right(ValidateQuestionOut(question, correlationId))
//      })
//
//  // Second stage
//
//  final case class LookupAnswersIn(question: String, correlationId: UUID)
//  final case class LookupAnswersOut(answer: String, correlationId: UUID)
//
//  def lookupAnswersStage(implicit ec: ExecutionContext,
//                         scheduler: Scheduler): Stage[LookupAnswersIn, LookupAnswersOut] =
//    Flow[ErrorOr[LookupAnswersIn]]
//      .mapAsyncUnordered(2) {
//        case Left(error) =>
//          Future.successful(Left(error))
//        case Right(LookupAnswersIn(question, correlationId)) =>
//          lookupAnswer(question)
//            .map(answer => LookupAnswersOut(answer, correlationId))
//            .recoverToEither(t => Error.LookupAnswerFailure(t, correlationId))
//      }
//
//  def lookupAnswer(question: String)(implicit ec: ExecutionContext,
//                                     scheduler: Scheduler): Future[String] =
//    after(4.seconds, scheduler)(Future.successful(question.reverse))
//
//  // Third stage, using a non-trivial graph and sometimes throwing random exceptions
//
//  final case class FourtyTwoIn(answer: String, correlationId: UUID)
//  final case class FourtyTwoOut(fourtyTwo: String, correlationId: UUID)
//
//  def fourtyTwo: Stage[FourtyTwoIn, FourtyTwoOut] =
//    Flow.fromGraph(GraphDSL.create() { implicit builder =>
//      import GraphDSL.Implicits._
//
//      val fromErrorOr =
//        builder.add(
//          Flow[ErrorOr[FourtyTwoIn]]
//            .map {
//              case Left(error) => (Some(error), None)
//              case Right(FourtyTwoIn(answer, correlationId)) =>
//                (None, Some((answer, correlationId)))
//            }
//        )
//      val unzip = builder.add(Unzip[Option[Error], Option[(String, UUID)]])
//      val failFast =
//        builder.add(Flow[Option[Error]].collect {
//          case e @ Some(_) => (e, Option.empty[(String, UUID)])
//        })
//      val collectSuccess = builder.add(Flow[Option[(String, UUID)]].collect { case Some(x) => x })
//      val fourtyTwo =
//        builder.add(Flow[(String, UUID)].map {
//          case (_, correlationId) =>
//            if (Random.nextInt(7) == 0) throw new Exception("Random exception again!")
//            else ("42", correlationId)
//        })
//      val lift = builder.add(Flow[(String, UUID)].map(x => (Option.empty[Error], Some(x))))
//      val merge =
//        builder.add(Merge[(Option[Error], Option[(String, UUID)])](2, eagerComplete = true))
//      val toErrorOr =
//        builder.add(Flow[(Option[Error], Option[(String, UUID)])].collect {
//          case (Some(error), None)              => Left(error)
//          case (None, Some((s, correlationId))) => Right(FourtyTwoOut(s, correlationId))
//        })
//
//      // format: off
//      fromErrorOr ~> unzip.in
//                     unzip.out0 ~> failFast                    ~>         merge.in(0)
//                     unzip.out1 ~> collectSuccess ~> fourtyTwo ~> lift ~> merge.in(1)
//                                                                          merge.out   ~> toErrorOr
//      // format: on
//
//      FlowShape(fromErrorOr.in, toErrorOr.out)
//    })
//
//  // Helpers
//
//  /**
//    * Extension methods for `Future`s.
//    */
//  implicit final class FutureOps[A](val fa: Future[A]) extends AnyVal {
//
//    /**
//      * Converts any `Future` into a successful one which wraps the value in an `Either`. Hence
//      * a successful `Future[A]` becomes a successful `Future[Right[B, A]]` and a failed `Future[A]`
//      * becomes a successful `Future[Left[B, A]]`.
//      *
//      * The use case is `FlowOps.mapAsync` which otherwise would fail the stream in case of a failed
//      * `Future`.
//      */
//    def recoverToEither[B](
//        toError: Throwable => B
//    )(implicit ec: ExecutionContext): Future[Either[B, A]] =
//      fa.transform {
//        case Failure(cause) => Success(Left(toError(cause)))
//        case Success(a)     => Success(Right(a))
//      }
//  }
//}
