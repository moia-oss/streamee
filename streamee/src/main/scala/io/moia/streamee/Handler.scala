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

import akka.stream.{ ActorMaterializer, Materializer, QueueOfferResult }
import akka.Done
import akka.actor.typed.scaladsl.adapter.UntypedActorSystemOps
import akka.stream.QueueOfferResult.{ Dropped, Enqueued }
import akka.stream.scaladsl.SourceQueueWithComplete
import io.moia.streamee.Handler.{ HandlerError, ProcessUnavailable }
import org.apache.logging.log4j.scala.Logging
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration.FiniteDuration

/**
  * Utilities for [[Handler]]s.
  */
object Handler extends Logging {

  /**
    * Signals that a request cannot be handled at this time.
    *
    * @param name name of the processor
    */
  final case class ProcessUnavailable(name: String)
      extends Exception(
        s"Running process $name cannot accept requests at this time!"
      )

  /**
    * Signals an unexpected result of calling [[Handler.handle]].
    *
    * @param cause the underlying erroneous `QueueOfferResult`, e.g. `Failure` or `QueueClosed`
    */
  final case class HandlerError(cause: QueueOfferResult)
      extends Exception(s"QueueOfferResult $cause was not expected!")
}

/**
  * Result of [[Process.runToHandler]] used to handle requests or shutdown.
  *
  * @tparam Req request type
  * @tparam Res response type
  */
final class Handler[Req, Res] private[streamee] (
    queue: SourceQueueWithComplete[(Req, Respondee[Res])],
    done: Future[Done],
    timeout: FiniteDuration,
    name: String,
)(implicit mat: Materializer, ec: ExecutionContext) {

  /**
    * Eventually handle a request.
    *
    * @param request request to be handled
    * @return `Future` for the response
    */
  def handle(request: Req): Future[Res] = {
    val response = Promise[Res]()
    val respondee =
      mat
        .asInstanceOf[ActorMaterializer]
        .system
        .spawnAnonymous(Respondee[Res](response, timeout))
    queue.offer((request, respondee)).flatMap {
      case Enqueued => response.future
      case Dropped  => Future.failed(ProcessUnavailable(name))
      case other    => Future.failed(HandlerError(other))
    }
  }

  /**
    * Shut down the running process of this [[Handler]]. Already accepted requests are still
    * handled, but new ones are dropped. The returened `Future` is completed once all requests
    * have been handled.
    *
    * @return `Future` signaling that all requests have been handled
    */
  def shutdown(): Future[Done] = {
    queue.complete()
    done
  }
}
