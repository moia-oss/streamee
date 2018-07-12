/*
 * Copyright (c) MOIA GmbH
 */

package io.moia.streamee
package demo

import akka.NotUsed
import akka.actor.CoordinatedShutdown.{ PhaseServiceRequestsDone, PhaseServiceUnbind, Reason }
import akka.actor.{ ActorSystem, CoordinatedShutdown, Scheduler }
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.server.{ Directives, Route }
import akka.stream.Materializer
import akka.stream.scaladsl.{ Flow, SourceQueue }
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport
import org.apache.logging.log4j.scala.Logging
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ ExecutionContext, Promise }
import scala.util.{ Failure, Success }

/**
  * API for this demo.
  */
object Api extends Logging {
  import CreateAccountLogic._

  final case class Config(address: String,
                          port: Int,
                          createAccountProcessorMaxLatency: FiniteDuration)

  private final case class SignUp(username: String, password: String, nickname: String)
  private final case class SignIn(username: String, password: String)

  private final object BindFailure extends Reason

  def apply(
      config: Config,
      createAccountLogic: Flow[CreateAccount, CreateAccountResult, NotUsed]
  )(implicit untypedSystem: ActorSystem, mat: Materializer): Unit = {
    import config._
    import untypedSystem.dispatcher

    implicit val scheduler: Scheduler = untypedSystem.scheduler
    val shutdown                      = CoordinatedShutdown(untypedSystem)

    // In real-world scenarios we probably would have more than one processor here!
    val (createAccountProcessor, createAccountShutdownSwitch) = Processor(createAccountLogic)

    Http()
      .bindAndHandle(
        route(createAccountProcessor, createAccountProcessorMaxLatency, shutdown),
        address,
        port
      )
      .onComplete {
        case Failure(cause) =>
          logger.error(s"Shutting down, because cannot bind to $address:$port!", cause)
          CoordinatedShutdown(untypedSystem).run(BindFailure)

        case Success(binding) =>
          logger.info(s"Listening for HTTP connections on ${binding.localAddress}")
          shutdown.addTask(PhaseServiceUnbind, "api.unbind") { () =>
            binding.unbind()
          }
          // This is important for not loosing in-flight requests!
          shutdown.addTask(PhaseServiceRequestsDone, "api.requests-done") { () =>
            createAccountShutdownSwitch.shutdown()
          }
      }
  }

  def route(
      createAccountProcessor: SourceQueue[(CreateAccount, Promise[CreateAccountResult])],
      createAccountProcessorMaxLatency: FiniteDuration,
      shutdown: CoordinatedShutdown
  )(implicit ec: ExecutionContext, scheduler: Scheduler): Route = {
    import CreateAccountLogic._
    import Directives._
    import ErrorAccumulatingCirceSupport._
    import ProcessorDirectives._
    import io.circe.generic.auto._

    pathSingleSlash {
      get {
        complete {
          OK
        }
      }
    } ~
    pathPrefix("accounts") {
      post {
        entity(as[CreateAccount]) {
          case createAccount @ CreateAccount(username) =>
            onProcessorSuccess(createAccount,
                               createAccountProcessor,
                               createAccountProcessorMaxLatency,
                               scheduler) {
              case UsernameInvalid =>
                complete(StatusCodes.BadRequest -> s"Username invalid: $username")
              case UsernameTaken =>
                complete(StatusCodes.Conflict -> s"Username taken: $username")
              case ac: AccountCreated =>
                complete(StatusCodes.Created -> ac)
            }
        }
      }
    }
  }
}
