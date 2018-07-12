/*
 * Copyright (c) MOIA GmbH
 */

package io.moia.streamee

import akka.actor.Scheduler
import akka.pattern.after
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ ExecutionContext, Future, Promise }

/**
  * Creates a promise which expires. See [[ExpiringPromise.apply]] for details.
  */
object ExpiringPromise {

  final case class PromiseExpired(timeout: FiniteDuration)
      extends Exception(s"Promise not completed successfully within $timeout!")

  /**
    * Creates a promise which expires. It is either completed successfully before the given
    * `timeout` or completed with a [[PromiseExpired]] exception.
    *
    * @param timeout maximum duration for the promise to be completed successfully
    * @param scheduler Akka scheduler needed for timeout handling
    * @param ec Scala execution context for timeout handling
    * @tparam R result type
    */
  def apply[R](timeout: FiniteDuration,
               scheduler: Scheduler)(implicit ec: ExecutionContext): Promise[R] = {
    val promisedR     = Promise[R]()
    val resultTimeout = after(timeout, scheduler)(Future.failed(PromiseExpired(timeout)))
    promisedR.tryCompleteWith(resultTimeout)
    promisedR
  }
}
