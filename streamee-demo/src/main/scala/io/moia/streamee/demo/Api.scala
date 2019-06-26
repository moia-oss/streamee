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
import akka.actor.{ CoordinatedShutdown, Scheduler, ActorSystem => UntypedSystem }
import akka.actor.CoordinatedShutdown.{ PhaseServiceUnbind, Reason }
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import org.apache.logging.log4j.scala.Logging
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.{ Failure, Success }

object Api extends Logging {

  final case class Config(hostname: String,
                          port: Int,
                          terminationDeadline: FiniteDuration,
                          wordShufflerHandler: WordShufflerHandlerConfig)

  final case class WordShufflerHandlerConfig(timeout: FiniteDuration, bufferSize: Int)

  private final object BindFailure extends Reason

  def apply(
      config: Config,
      wordShuffler: Process[WordShuffler.ShuffleWord,
                            WordShuffler.WordShuffled,
                            WordShuffler.WordShuffled]
  )(implicit untypedSystem: UntypedSystem, mat: Materializer, scheduler: Scheduler): Unit = {
    import Handler.processUnavailableHandler
    import config._
    import untypedSystem.dispatcher

    val shutdown = CoordinatedShutdown(untypedSystem)

    val wordShufflerHandler = {
      import config.wordShufflerHandler._
      Process.runToHandler(wordShuffler, timeout, bufferSize, "word-shuffler")
    }

    Http()
      .bindAndHandle(route(wordShufflerHandler), hostname, port)
      .onComplete {
        case Failure(cause) =>
          logger.error(s"Shutting down, because cannot bind to $hostname:$port!", cause)
          shutdown.run(BindFailure)

        case Success(binding) =>
          logger.info(s"Listening for HTTP connections on ${binding.localAddress}")
          shutdown.addTask(PhaseServiceUnbind, "api.unbind") { () =>
            binding.terminate(terminationDeadline).map(_ => Done)
          }
      }
  }

  def route(
      wordShufflerHandler: Handler[WordShuffler.ShuffleWord, WordShuffler.WordShuffled]
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
    path("shuffle") {
      import WordShuffler._
      post {
        entity(as[ShuffleWord]) { shuffleWord =>
          onSuccess(wordShufflerHandler.handle(shuffleWord)) {
            case WordShuffled(original, result) => complete(s"$original -> $result")
          }
        }
      }
    }
  }
}
