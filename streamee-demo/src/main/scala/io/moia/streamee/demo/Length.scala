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
package demo

import akka.NotUsed
import akka.actor.Scheduler
import akka.stream.Materializer
import akka.stream.scaladsl.{ Flow, FlowWithContext }
import io.moia.streamee.intoable.{ FlowOps, IntoableProcess, IntoableSink }
import scala.concurrent.{ ExecutionContext, Promise }
import scala.concurrent.duration.FiniteDuration

/**
  * A trivial domain logic process demoing the use of `into`.
  */
object Length {

  type Process = Flow[String, String, NotUsed]

  final case class Config(retryTimeout: FiniteDuration)

  def apply(config: Config, intoableLength: IntoableSink[String, Int])(
      implicit mat: Materializer,
      ec: ExecutionContext,
      scheduler: Scheduler
  ): Flow[String, String, NotUsed] =
    Flow[String].into(intoableLength, 42).map(_.toString)
}

/**
  * A trivial "intoable" process.
  */
object IntoableLength {

  def apply(): IntoableProcess[String, Int] =
    FlowWithContext[Promise[Int], String].map(_.length)
}
