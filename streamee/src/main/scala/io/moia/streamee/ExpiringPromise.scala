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
    * @tparam R response type
    */
  def apply[R](timeout: FiniteDuration,
               scheduler: Scheduler)(implicit ec: ExecutionContext): Promise[R] = {
    val promisedR       = Promise[R]()
    val responseTimeout = after(timeout, scheduler)(Future.failed(PromiseExpired(timeout)))
    promisedR.tryCompleteWith(responseTimeout)
    promisedR
  }
}
