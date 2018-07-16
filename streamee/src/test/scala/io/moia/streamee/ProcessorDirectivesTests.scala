package io.moia.streamee

import akka.actor.CoordinatedShutdown
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.testkit.{ RouteTest, TestFrameworkInterface }
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{ Flow, SourceQueue }
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt
import utest._

object ProcessorDirectivesTests extends TestSuite with RouteTest with TestFrameworkInterface {
  import Directives._
  import ProcessorDirectives._

  private val toUpperCase = Flow[String].map(_.length % 2 == 0)

  private val shutdown = CoordinatedShutdown(system)

  override def tests: Tests =
    Tests {
      'happyPath - {
        val processor = Processor(toUpperCase, 42, shutdown)

        Post("/", "12") ~> route(processor) ~> check {
          val actualStatus = status
          assert(actualStatus == StatusCodes.Created)
        }

        Post("/", "1") ~> route(processor) ~> check {
          val actualStatus = status
          assert(actualStatus == StatusCodes.BadRequest)
        }
      }

      // TODO How to fire multiple concurrent requests with RouteTest?
//      'serviceUnavailable - {
//      }

      'serviceTooSlow - {
        val processor =
          Processor(toUpperCase.delay(1.second, OverflowStrategy.backpressure), 42, shutdown)

        Post("/", "12") ~> route(processor) ~> check {
          val actualStatus = status
          assert(actualStatus == StatusCodes.InternalServerError)
        }
      }
    }

  override def utestAfterAll(): Unit = {
    system.terminate()
    super.utestAfterAll()
  }

  override def failTest(msg: String): Nothing =
    throw new Exception(s"Test failed: $msg")

  private def route(processor: SourceQueue[(String, Promise[Boolean])]) =
    post {
      entity(as[String]) { command =>
        onProcessorSuccess(command, processor, 100.milliseconds, system.scheduler) { result =>
          if (result)
            complete(StatusCodes.Created)
          else
            complete(StatusCodes.BadRequest)
        }
      }
    }
}
