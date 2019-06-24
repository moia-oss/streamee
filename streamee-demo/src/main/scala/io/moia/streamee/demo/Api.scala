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

import akka.{ Done, NotUsed }
import akka.actor.{ CoordinatedShutdown, Scheduler, ActorSystem => UntypedSystem }
import akka.actor.CoordinatedShutdown.{ PhaseServiceUnbind, Reason }
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import io.moia.streamee.processor.Processor
import org.apache.logging.log4j.scala.Logging
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.{ Failure, Success }

/**
  * API for this demo.
  */
object Api extends Logging {

  final case class Config(hostname: String,
                          port: Int,
                          terminationDeadline: FiniteDuration,
                          processorTimeout: FiniteDuration,
                          processorBufferSize: Int)

  private final case class Request(question: String)

  private final object BindFailure extends Reason

  def apply(
      config: Config,
      fourtyTwo: FourtyTwo.Process,
      length: Length.Process
  )(implicit untypedSystem: UntypedSystem, mat: Materializer, scheduler: Scheduler): Unit = {
    import Processor.processorUnavailableHandler
    import config._
    import untypedSystem.dispatcher

    val shutdown = CoordinatedShutdown(untypedSystem)

    val fourtyTwoProcessor =
      Processor(fourtyTwo, processorTimeout, "per-request", 1).registerWithCoordinatedShutdown(
        shutdown
      )

    val lengthProcessor =
      Processor(length, processorTimeout, "length", 1).registerWithCoordinatedShutdown(shutdown)

    val errorProcessor =
      Processor(
        Process[NotUsed, NotUsed]().map(_ => throw new Exception("ERROR")),
        processorTimeout,
        "error",
        1
      )

    Http()
      .bindAndHandle(
        route(fourtyTwoProcessor, lengthProcessor, errorProcessor),
        hostname,
        port
      )
      .onComplete {
        case Failure(cause) =>
          logger.error(s"Shutting down, because cannot bind to $hostname:$port!", cause)
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
      lengthProcessor: Processor[String, String],
      errorProcessor: Processor[NotUsed, NotUsed]
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
    path("length") {
      get {
        complete {
          lengthProcessor.process("o" * 42)
        }
      }
    } ~
    path("error") {
      get {
        complete {
          errorProcessor.process(NotUsed)
        }
      }
    }
  }
}
