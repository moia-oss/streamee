/*
 * Copyright (c) MOIA GmbH
 */

package io.moia.streamee
package demo

import akka.NotUsed
import akka.actor.Scheduler
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Flow
import akka.util.Timeout
import scala.concurrent.Future
import scala.concurrent.duration.{ DurationInt, FiniteDuration }

/**
  * Deom logic pipeline for creating an account. See [[CreateAccountLogic.apply]] for details.
  */
object CreateAccountLogic {

  final case class CreateAccount(username: String)

  sealed trait CreateAccountResult
  final case object UsernameInvalid                 extends CreateAccountResult
  final case object UsernameTaken                   extends CreateAccountResult
  final case class AccountCreated(username: String) extends CreateAccountResult

  final case class Config(accountsAskTimeout: FiniteDuration)

  /**
    * Demo logic pipeline for creating an account:
    * First the given [[Accounts]] actor is asked to create an account.
    * Then an artificial delay is used to simulate communicating with an external service.
    * Finally the reply from the actor is translated into a logic result (only 1:1 in this trivial case).
    *
    * @param config configuration object for this logic pipeline
    * @param accounts persistent actor for accounts
    * @param scheduler Akka scheduler needed for asking
    */
  def apply(config: Config, accounts: ActorRef[Accounts.Command])(
      implicit scheduler: Scheduler
  ): Flow[CreateAccount, CreateAccountResult, NotUsed] =
    Flow[CreateAccount]
      .mapAsync(42) { // TODO Use configurable value!
        case CreateAccount(username) =>
          if (username.isEmpty)
            Future.successful(UsernameInvalid)
          else {
            implicit val timeout: Timeout = config.accountsAskTimeout
            accounts ? Accounts.createAccount(username)
          }
      }
      .async
      .delay(5.seconds, OverflowStrategy.backpressure)
      .map {
        case Accounts.UsernameTaken            => UsernameTaken
        case Accounts.AccountCreated(username) => AccountCreated(username)
      }
}
