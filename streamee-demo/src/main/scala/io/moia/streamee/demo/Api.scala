package io.moia.streamee
package demo

import akka.actor.CoordinatedShutdown.{ PhaseServiceUnbind, Reason }
import akka.actor.{ ActorSystem, CoordinatedShutdown, Scheduler }
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.server.{ Directives, Route }
import akka.stream.Materializer
import akka.stream.scaladsl.SourceQueue
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport
import org.apache.logging.log4j.scala.Logging
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ ExecutionContext, Promise }
import scala.util.{ Failure, Success }

/**
  * API for this demo.
  */
object Api extends Logging {

  final case class Config(address: String, port: Int, demoProcessorTimeout: FiniteDuration)

  private final case class Entity(s: String)

  private final object BindFailure extends Reason

  def apply(
      config: Config,
      demoProcessor: Processor[String, String]
  )(implicit untypedSystem: ActorSystem, mat: Materializer): Unit = {
    import config._
    import untypedSystem.dispatcher

    implicit val scheduler: Scheduler = untypedSystem.scheduler

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
      demoProcessor: SourceQueue[(String, Promise[String])],
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
        entity(as[Entity]) {
          case Entity(s) =>
            onProcessorSuccess(s, demoProcessor, demoProcessorTimeout, scheduler) {
              case s if s.isEmpty =>
                complete(StatusCodes.BadRequest -> "Empty entity!")
              case s if s.startsWith("taxi") =>
                complete(StatusCodes.Conflict -> "We don't like taxis ;-)")
              case s =>
                complete(StatusCodes.Created -> s)
            }
        }
      }
    }
  }
}
