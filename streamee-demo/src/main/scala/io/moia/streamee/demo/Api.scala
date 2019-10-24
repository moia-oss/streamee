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

import akka.Done
import akka.actor.{ CoordinatedShutdown, ActorSystem => ClassicSystem }
import akka.actor.CoordinatedShutdown.{ PhaseServiceUnbind, Reason }
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes.{ OK, ServiceUnavailable }
import akka.http.scaladsl.server.{ ExceptionHandler, Route }
import akka.http.scaladsl.server.Directives.complete
import akka.stream.Materializer
import org.apache.logging.log4j.scala.Logging
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.{ Failure, Success }

object Api extends Logging {

  final case class Config(interface: String, port: Int, terminationDeadline: FiniteDuration)

  private final object BindFailure extends Reason

  def apply(
      config: Config,
      textShufflerProcessor: FrontProcessor[TextShuffler.ShuffleText, TextShuffler.TextShuffled]
  )(implicit classicSystem: ClassicSystem, mat: Materializer): Unit = {
    import classicSystem.dispatcher
    import config._

    implicit val processUnavailableHandler: ExceptionHandler =
      ExceptionHandler {
        case FrontProcessor.ProcessorUnavailable(name) =>
          complete(ServiceUnavailable -> s"Processor $name unavailable!")
      }

    val shutdown = CoordinatedShutdown(classicSystem)

    Http()
      .bindAndHandle(route(textShufflerProcessor), interface, port)
      .onComplete {
        case Failure(cause) =>
          logger.error(s"Shutting down, because cannot bind to $interface:$port!", cause)
          shutdown.run(BindFailure)

        case Success(binding) =>
          logger.info(s"Listening for HTTP connections on ${binding.localAddress}")
          shutdown.addTask(PhaseServiceUnbind, "api.unbind") { () =>
            binding.terminate(terminationDeadline).map(_ => Done)
          }
      }
  }

  def route(
      textShufflerProcessor: FrontProcessor[TextShuffler.ShuffleText, TextShuffler.TextShuffled]
  )(implicit ec: ExecutionContext): Route = {
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
    path("shuffle") {
      import TextShuffler._
      post {
        entity(as[ShuffleText]) { shuffleText =>
          onSuccess(textShufflerProcessor.offer(shuffleText)) {
            case TextShuffled(original, result) => complete(s"$original -> $result")
          }
        }
      }
    }
  }
}
