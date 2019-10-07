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

package io.moia.streamee4
package processor

import akka.Done
import akka.actor.Scheduler
import akka.stream.QueueOfferResult.{ Dropped, Enqueued }
import akka.stream.scaladsl.{ Flow, Keep, Sink, Source }
import akka.stream.{ Materializer, OverflowStrategy }
import io.moia.streamee4.processor.Processor.{ ProcessorError, ProcessorUnavailable }
import scala.concurrent.duration.{ Duration, FiniteDuration }
import scala.concurrent.{ ExecutionContext, Future, Promise }

private final class PerRequestProcessor[A, B](
    process: Flow[A, B, Any],
    timeout: FiniteDuration,
    name: String
)(implicit ec: ExecutionContext, mat: Materializer, scheduler: Scheduler)
    extends Processor[A, B] {

  require(timeout > Duration.Zero, s"timeout must be positive, but was $timeout!")

  private val (queue, done) =
    Source
      .queue[(A, Promise[B])](1, OverflowStrategy.dropNew) // No need to use a large buffer, beause a substream is run for each request!
      .toMat(Sink.foreach {
        case (request, response) =>
          response.completeWith(Source.single(request).via(process).runWith(Sink.head))
      })(Keep.both)
      .run()

  override def process(request: A): Future[B] = {
    val response = ExpiringPromise[B](timeout, s"from processor $name for request $request")
    queue
      .offer((request, response))
      .flatMap {
        case Enqueued => response.future
        case Dropped  => Future.failed(ProcessorUnavailable(name))
        case other    => Future.failed(ProcessorError(other))
      }
  }

  override def shutdown(): Future[Done] = queue.watchCompletion()

}
