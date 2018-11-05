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

import akka.actor.typed.{ ActorRef, Behavior }
import akka.actor.typed.scaladsl.Behaviors
import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration

object RespondeeFactory {

  sealed trait Command[A]
  final case class CreateRespondee[A](response: Promise[A],
                                      responseTimeout: FiniteDuration,
                                      replyTo: ActorRef[RespondeeCreated[A]])
      extends Command[A]
  final case class RespondeeCreated[A](respondee: Respondee[A])

  def apply[A](): Behavior[Command[A]] =
    Behaviors.receive {
      case (context, CreateRespondee(response, responseTimeout, replyTo)) =>
        val respondee = context.spawnAnonymous(Respondee(response, responseTimeout))
        replyTo ! RespondeeCreated(respondee)
        Behaviors.same
    }
}

object Respondee {

  sealed trait Command[+A]
  private[streamee] final case class Response[A](a: A) extends Command[A]
  private final case object Timeout                    extends Command[Nothing]

  final case class ResponseTimeoutException(timeout: FiniteDuration)
      extends Exception(s"Not responded within $timeout!")

  def apply[A](response: Promise[A], responseTimeout: FiniteDuration): Behavior[Command[A]] =
    Behaviors.withTimers { timers =>
      timers.startSingleTimer("response-timeout", Timeout, responseTimeout)

      Behaviors.receiveMessage {
        case Timeout =>
          response.failure(ResponseTimeoutException(responseTimeout))
          Behaviors.stopped

        case Response(a: A @unchecked) =>
          response.success(a)
          Behaviors.stopped
      }
    }
}
