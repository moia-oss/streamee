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
import org.apache.logging.log4j.scala.Logging
import scala.concurrent.duration.{ DurationInt, FiniteDuration }
import scala.concurrent.{ ExecutionContext, Future, Promise }

object DemoPipeline extends Logging {

  private def step(name: String, duration: FiniteDuration, scheduler: Scheduler)(
      s: String
  )(implicit ec: ExecutionContext) = {
    logger.debug(s"Before $name")
    val p = Promise[String]()
    p.tryCompleteWith(after(duration, scheduler) {
      logger.debug(s"After $name")
      Future.successful(s)
    })
    p.future
  }

  /**
    * Simple domain logic pipeline for demo purposes.
    *
    * The pipeline is comprised of two stages (aka steps or tasks). Each of these performs its work
    * asynchronously, hence `mapAsync` is used. Typical real-world examples for such stages are
    * calls to external services (e.g. via HTTP or gRPC) or interacting with actors in a
    * request-response way (via the ask pattern).
    *
    * The value 1 for the `parallelism` of `mapAsync` is chosen for demonstration purposes only: it
    * allows for easily showing the effect of backpressure. For real-world applications usually a
    * higher value would be suitable.
    */
  def apply(scheduler: Scheduler)(implicit ec: ExecutionContext): Flow[String, String, NotUsed] =
    Flow[String]
      .mapAsync(1)(step("step1", 2.seconds, scheduler))
      .mapAsync(1)(step("step2", 2.seconds, scheduler))
}
