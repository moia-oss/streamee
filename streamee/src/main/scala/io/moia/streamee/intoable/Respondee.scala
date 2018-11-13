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
package intoable

import akka.actor.typed.scaladsl.{ ActorContext, Behaviors }
import akka.actor.typed.{ ActorRef, Behavior }
import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration

object Respondee {

  sealed trait Command
  final case class Response[A] private (a: A) extends Command
  private final case object Timeout           extends Command

  final case class ResponseTimeoutException(timeout: FiniteDuration, hint: String = "")
      extends Exception(s"Not responded within $timeout! hint='$hint'")

  def apply[A](response: Promise[A],
               responseTimeout: FiniteDuration,
               hint: String = ""): Behavior[Response[A]] =
    Behaviors
      .withTimers[Command] { timers =>
        timers.startSingleTimer("response-timeout", Timeout, responseTimeout)

        Behaviors.receiveMessage {
          case Timeout =>
            response.failure(ResponseTimeoutException(responseTimeout, hint))
            Behaviors.stopped

          case Response(a: A @unchecked) =>
            response.success(a)
            Behaviors.stopped
        }
      }
      .narrow
}

object RespondeeFactory {

  final case class CreateRespondee[A](response: Promise[A],
                                      responseTimeout: FiniteDuration,
                                      replyTo: ActorRef[RespondeeCreated[A]],
                                      hint: String = "")
  final case class RespondeeCreated[A](respondee: ActorRef[Respondee.Response[A]])

  implicit def spawn[A, B](implicit context: ActorContext[B]): ActorRef[CreateRespondee[A]] =
    context.spawnAnonymous(RespondeeFactory[A]())

  def apply[A](): Behavior[CreateRespondee[A]] =
    Behaviors.receive {
      case (context, CreateRespondee(response, responseTimeout, replyTo, hint)) =>
        val respondee = context.spawnAnonymous(Respondee(response, responseTimeout, hint))
        replyTo ! RespondeeCreated(respondee)
        Behaviors.same
    }
}
