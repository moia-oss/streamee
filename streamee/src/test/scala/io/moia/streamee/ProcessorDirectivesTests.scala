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

import akka.actor.CoordinatedShutdown
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.testkit.{ RouteTest, TestFrameworkInterface }
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{ Flow, SourceQueue }
import akka.testkit.TestDuration
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
        val processor = Processor(toUpperCase, ProcessorSettings(system), shutdown)

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
          Processor(toUpperCase.delay(1.second, OverflowStrategy.backpressure),
                    ProcessorSettings(system),
                    shutdown)

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
      entity(as[String]) { request =>
        onProcessorSuccess(request, processor, 100.milliseconds.dilated, system.scheduler) {
          response =>
            if (response)
              complete(StatusCodes.Created)
            else
              complete(StatusCodes.BadRequest)
        }
      }
    }
}
