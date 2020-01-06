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

  /**
    * Creates a promise which expires. It is either completed successfully before the given
    * `timeout` or completed with a [[PromiseExpired]] exception.
    *
    * @param timeout maximum duration for the promise to be completed successfully
    * @param hint optional information to be passed to the [[PromiseExpired]]
    * @tparam A result type
    */
  def apply[A](timeout: FiniteDuration, hint: String = "")(implicit ec: ExecutionContext,
                                                           scheduler: Scheduler): Promise[A] = {
    val result          = Promise[A]()
    val responseTimeout = after(timeout, scheduler)(Future.failed(PromiseExpired(timeout, hint)))
    result.completeWith(responseTimeout)
    result
  }
}

/**
  * Exception which signals that a `Promise` has expired.
  *
  * @param timeout maximum duration for the promise to be completed successfully
  * @param hint optional information about the expired promise
  */
final case class PromiseExpired(timeout: FiniteDuration, hint: String = "")
    extends Exception(s"Promise $hint expired after $timeout!")
