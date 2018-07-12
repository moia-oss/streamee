/*
 * Copyright (c) MOIA GmbH
 */

package io.moia.streamee.demo

import akka.NotUsed
import akka.actor.Scheduler
import akka.pattern.after
import akka.stream.scaladsl.Flow
import org.apache.logging.log4j.scala.Logging
import scala.concurrent.duration.{ DurationInt, FiniteDuration }
import scala.concurrent.{ ExecutionContext, Future, Promise }

object DemoLogic extends Logging {

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

  def apply(scheduler: Scheduler)(implicit ec: ExecutionContext): Flow[String, String, NotUsed] =
    Flow[String]
      .mapAsync(1)(step("step1", 2.second, scheduler))
      .mapAsync(1)(step("step2", 2.second, scheduler))
}
