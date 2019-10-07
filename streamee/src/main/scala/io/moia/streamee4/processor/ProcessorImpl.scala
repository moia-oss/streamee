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

import akka.stream.{ ActorAttributes, Materializer, OverflowStrategy }
import akka.stream.scaladsl.{ Keep, Sink, Source }
import akka.stream.QueueOfferResult.{ Dropped, Enqueued }
import akka.Done
import akka.actor.Scheduler
import io.moia.streamee4.processor.PermanentProcessor.resume
import io.moia.streamee4.processor.Processor.{ ProcessorError, ProcessorUnavailable }
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration.{ Duration, FiniteDuration }

private final class ProcessorImpl[Req, Res](
    process: Process[Req, Res],
    timeout: FiniteDuration,
    name: String,
    bufferSize: Int
)(implicit ec: ExecutionContext, mat: Materializer, scheduler: Scheduler)
    extends Processor[Req, Res] {

  require(timeout > Duration.Zero, s"timeout must be positive, but was $timeout!")
  require(bufferSize > 0, s"bufferSize must be positive, but was $bufferSize!")

  private val (queue, done) =
    Source
      .queue[(Req, Promise[Res])](bufferSize, OverflowStrategy.dropNew)
      .via(process)
      .toMat(Sink.foreach { case (response, p) => p.trySuccess(response) })(Keep.both)
      .withAttributes(ActorAttributes.supervisionStrategy(resume(name)))
      .run()

  override def process(request: Req): Future[Res] = {
    val response = ExpiringPromise[Res](timeout, s"from processor $name for request $request")
    queue.offer((request, response)).flatMap {
      case Enqueued => response.future
      case Dropped  => Future.failed(ProcessorUnavailable(name))
      case other    => Future.failed(ProcessorError(other))
    }
  }

  override def shutdown(): Future[Done] = queue.watchCompletion()
}
