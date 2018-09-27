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

import akka.NotUsed
import akka.actor.Scheduler
import akka.pattern.after
import akka.stream.scaladsl.Flow
import java.util.UUID
import org.apache.logging.log4j.scala.Logging
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.DurationInt

object DemoProcess extends Logging {

  final case class Request(question: String, correlationId: UUID = UUID.randomUUID()) {
    require(question.nonEmpty, "question must not be empty!")
  }
  final case class Response(answer: String, correlationId: UUID = UUID.randomUUID())

  /**
    * Simple domain logic process for demo purposes. Always answers with "42" ;-)
    *
    * The process is comprised of two stages (aka steps or tasks). Each of these performs its work
    * asynchronously and after an artificial delay, hence `mapAsync` is used. Typical real-world
    * examples for such stages are calls to external services (e.g. via HTTP or gRPC) or interacting
    * with actors in a request-response way (via the ask pattern).
    *
    * The value 1 for the `parallelism` of `mapAsync` is chosen for demonstration purposes only: it
    * allows for easily showing the effect of backpressure. For real-world applications usually a
    * higher value would be suitable.
    */
  def apply()(implicit ec: ExecutionContext,
              scheduler: Scheduler): Flow[Request, Response, NotUsed] =
    Flow[Request]
      .mapAsync(1) {
        case request @ Request(question, _) =>
          after(2.seconds, scheduler) {
            Future.successful((request, question.length * 42))
          }
      }
      .mapAsync(1) {
        case (Request(question, correlationId), n) =>
          after(2.seconds, scheduler) {
            Future.successful(Response((n / question.length).toString, correlationId))
          }
      }
}
