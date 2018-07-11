/*
 * Copyright (c) MOIA GmbH
 */

package io.moia.streamee
package demo

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ ActorRef, Behavior }
import org.apache.logging.log4j.scala.Logging

/**
  * Simple repository like actor for storing accounts. Should be persistent and offer a wider
  * protocol, but it's just fine for the purpose of this demo.
  */
object Accounts extends Logging {

  // Message protocol – start

  sealed trait Command

  final case class CreateAccount(username: String, replyTo: ActorRef[CreateAccountReply])
      extends Command

  sealed trait CreateAccountReply
  final case object UsernameTaken                   extends CreateAccountReply
  final case class AccountCreated(username: String) extends CreateAccountReply

  // Message protocol – end

  def apply(usernames: Set[String] = Set.empty): Behavior[Command] =
    Behaviors.receiveMessage {
      case CreateAccount(username, replyTo) =>
        if (usernames.contains(username)) {
          logger.debug(s"Username taken: $username")
          replyTo ! UsernameTaken
          Behaviors.same
        } else {
          logger.debug(s"Account created: $username")
          replyTo ! AccountCreated(username)
          Accounts(usernames + username)
        }
    }

  def createAccount(username: String)(replyTo: ActorRef[CreateAccountReply]): CreateAccount =
    CreateAccount(username, replyTo)
}
