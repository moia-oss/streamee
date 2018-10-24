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

import akka.actor.{ CoordinatedShutdown, Scheduler, ActorSystem => UntypedSystem }
import akka.actor.CoordinatedShutdown.{ PhaseServiceUnbind, Reason }
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter.TypedActorSystemOps
import org.apache.logging.log4j.scala.Logging
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success }

/**
  * API for this demo.
  */
object Api extends Logging {

  final case class Config(address: String,
                          port: Int,
                          terminationDeadline: FiniteDuration,
                          processorTimeout: FiniteDuration,
                          processorBufferSize: Int)

  private final case class Request(question: String)

  private final object BindFailure extends Reason

  def apply(config: Config, fourtyTwo: FourtyTwo.Process)(implicit system: ActorSystem[_],
                                                          mat: Materializer,
                                                          scheduler: Scheduler): Unit = {
    import Processor.processorUnavailableHandler
    import config._
    import untypedSystem.dispatcher

    implicit val untypedSystem: UntypedSystem = system.toUntyped

    val fourtyTwoProcessor =
      Processor.perRequest(fourtyTwo,
                           processorTimeout,
                           "per-request",
                           CoordinatedShutdown(untypedSystem))

    val fourtyTwoCorrelatedProcessor =
      Processor.permanent(
        FourtyTwoCorrelated(),
        processorTimeout,
        "permanant",
        processorBufferSize,
        CoordinatedShutdown(untypedSystem)
      )(_.correlationId, _.fold(_.correlationId, _.correlationId))

    Http()
      .bindAndHandle(
        route(fourtyTwoProcessor, fourtyTwoCorrelatedProcessor),
        address,
        port
      )
      .onComplete {
        case Failure(cause) =>
          logger.error(s"Shutting down, because cannot bind to $address:$port!", cause)
          CoordinatedShutdown(untypedSystem).run(BindFailure)

        case Success(binding) =>
          logger.info(s"Listening for HTTP connections on ${binding.localAddress}")
          CoordinatedShutdown(untypedSystem).addTask(PhaseServiceUnbind, "api.unbind") { () =>
            binding.terminate(terminationDeadline).map(_ => Done)
          }
      }
  }

  def route(
      fourtyTwoProcessor: Processor[FourtyTwo.Request, FourtyTwo.ErrorOr[FourtyTwo.Response]],
      fourtyTwoCorrelatedProcessor: Processor[
        FourtyTwoCorrelated.Request,
        FourtyTwoCorrelated.ErrorOr[FourtyTwoCorrelated.Response]
      ]
  )(implicit ec: ExecutionContext, scheduler: Scheduler): Route = {
    import akka.http.scaladsl.server.Directives._
    import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
    import io.circe.generic.auto._

    pathSingleSlash {
      get {
        complete {
          OK
        }
      }
    } ~
    path("fourty-two") {
      post {
        entity(as[Request]) {
          case Request(question) =>
            onSuccess(fourtyTwoProcessor.process(FourtyTwo.Request(question))) {
              case Left(FourtyTwo.Error.EmptyQuestion) =>
                complete(StatusCodes.BadRequest -> "Empty question not allowed!")

              case Left(_) =>
                complete(StatusCodes.InternalServerError -> "Oops, something bad happended :-(")

              case Right(FourtyTwo.Response(answer)) =>
                complete(StatusCodes.Created -> s"The answer is $answer")
            }
        }
      }
    } ~
    path("fourty-two-correlated") {
      post {
        entity(as[Request]) {
          case Request(question) =>
            onSuccess(fourtyTwoCorrelatedProcessor.process(FourtyTwoCorrelated.Request(question))) {
              case Left(FourtyTwoCorrelated.Error.EmptyQuestion(_)) =>
                complete(StatusCodes.BadRequest -> "Empty question not allowed!")

              case Left(_) =>
                complete(StatusCodes.InternalServerError -> "Oops, something bad happended :-(")

              case Right(FourtyTwoCorrelated.Response(answer, _)) =>
                complete(StatusCodes.Created -> s"The answer is $answer")
            }
        }
      }
    }
  }
}
