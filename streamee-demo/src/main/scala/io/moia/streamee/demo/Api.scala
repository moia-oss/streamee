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

import akka.Done
import akka.actor.{ CoordinatedShutdown, ActorSystem => ClassicSystem }
import akka.actor.CoordinatedShutdown.{ PhaseServiceUnbind, Reason }
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes.{ BadRequest, OK }
import akka.http.scaladsl.server.Route
import io.moia.streamee.FrontProcessor
import org.slf4j.LoggerFactory
import scala.concurrent.duration.FiniteDuration
import scala.util.{ Failure, Success }

object Api {

  type TextShufflerProcessor =
    FrontProcessor[TextShuffler.ShuffleText, Either[TextShuffler.Error, TextShuffler.TextShuffled]]

  final case class Config(interface: String, port: Int, terminationDeadline: FiniteDuration)

  private final object BindFailure extends Reason

  private val logger = LoggerFactory.getLogger(getClass)

  def apply(config: Config, textShufflerProcessor: TextShufflerProcessor)(
      implicit classicSystem: ClassicSystem
  ): Unit = {
    import FrontProcessor.processorUnavailableHandler
    import classicSystem.dispatcher
    import config._

    val shutdown = CoordinatedShutdown(classicSystem)

    Http()
      .bindAndHandle(route(textShufflerProcessor), interface, port)
      .onComplete {
        case Failure(cause) =>
          if (logger.isErrorEnabled)
            logger.error(s"Shutting down, because cannot bind to $interface:$port!", cause)
          shutdown.run(BindFailure)

        case Success(binding) =>
          if (logger.isInfoEnabled)
            logger.info(s"Listening for HTTP connections on ${binding.localAddress}")
          shutdown.addTask(PhaseServiceUnbind, "api.unbind") { () =>
            binding.terminate(terminationDeadline).map(_ => Done)
          }
      }
  }

  def route(textShufflerProcessor: TextShufflerProcessor): Route = {
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
            case Left(Error.EmptyText)                 => complete(BadRequest -> "Empty text!")
            case Left(Error.InvalidText)               => complete(BadRequest -> "Invalid text!")
            case Right(TextShuffled(original, result)) => complete(s"$original -> $result")
          }
        }
      }
    }
  }
}
