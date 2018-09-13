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

import akka.actor.{ ActorSystem, CoordinatedShutdown, Scheduler }
import akka.actor.CoordinatedShutdown.{ PhaseServiceUnbind, Reason }
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.server.{ Directives, Route }
import akka.stream.Materializer
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport
import org.apache.logging.log4j.scala.Logging
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success }

/**
  * API for this demo.
  */
object Api extends Logging {

  final case class Config(address: String, port: Int, demoProcessorTimeout: FiniteDuration)

  private final case class Entity(s: String)

  private final object BindFailure extends Reason

  def apply(config: Config)(implicit untypedSystem: ActorSystem, mat: Materializer): Unit = {
    import config._
    import untypedSystem.dispatcher

    implicit val scheduler: Scheduler = untypedSystem.scheduler

val demoProcessor =
  Processor(DemoProcess(scheduler)(untypedSystem.dispatcher),
            ProcessorSettings(untypedSystem),
            CoordinatedShutdown(untypedSystem))

    Http()
      .bindAndHandle(
        route(demoProcessor, demoProcessorTimeout),
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
            binding.unbind()
          }
      }
  }

  def route(
      demoProcessor: Processor[DemoProcess.Request, DemoProcess.Response],
      demoProcessorTimeout: FiniteDuration
  )(implicit ec: ExecutionContext, scheduler: Scheduler): Route = {
    import Directives._
    import ErrorAccumulatingCirceSupport._
    import ProcessorDirectives._
    import io.circe.generic.auto._

    pathSingleSlash {
      get {
        complete {
          OK
        }
      } ~
      post {
        entity(as[DemoProcess.Request]) { request =>
          onProcessorSuccess(request, demoProcessor, demoProcessorTimeout, scheduler) {
            case DemoProcess.Response(_, n) if n == 42 =>
              complete(StatusCodes.BadRequest -> "Request must not have n == 1!")

            case DemoProcess.Response(_, n) =>
              complete(StatusCodes.Created -> n)
          }
        }
      }
    }
  }
}
