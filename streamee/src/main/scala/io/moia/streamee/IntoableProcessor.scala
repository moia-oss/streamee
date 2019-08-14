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

import akka.stream.{ ActorAttributes, KillSwitches, Materializer, SinkRef, Supervision }
import akka.stream.scaladsl.{ Keep, MergeHub, Sink, StreamRefs }
import akka.Done
import akka.actor.CoordinatedShutdown
import org.apache.logging.log4j.scala.Logging
import scala.concurrent.Future

object IntoableProcessor {

  /**
    * Create a [[FrontProcessor]]: run the given `process` such that it can be used with the `into`
    * stream extension operator. Notice that shutting down might result in dropping (losing)
    * `bufferSize` number of requsts!
    *
    * @param process top-level domain logic process from request to response
    * @param name name, used for logging
    * @param bufferSize optional size of the buffer of the used `MergeHub.source`; defaults to 1; must be positive!
    * @param phase identifier for a phase of `CoordinatedShutdown`; defaults to ""service-stop""; must be defined in configufation!
    * @tparam Req request type
    * @tparam Res response type
    * @return [[IntoableProcessor]]
    */
  def apply[Req, Res](
      process: Process[Req, Res, Res],
      name: String,
      bufferSize: Int = 1,
      phase: String = CoordinatedShutdown.PhaseServiceStop
  )(implicit mat: Materializer): IntoableProcessor[Req, Res] =
    new IntoableProcessor(process, name, bufferSize, phase)
}

/**
  * Run the given `process` such that it can be used with the `into` stream extension operator.
  * Notice that shutting down might result in dropping (losing) `bufferSize` number of requsts!
  */
final class IntoableProcessor[Req, Res] private (
    process: Process[Req, Res, Res],
    name: String,
    bufferSize: Int = 1,
    phase: String
)(implicit mat: Materializer)
    extends Logging {
  require(
    bufferSize > 0,
    s"bufferSize for processor $name must be > 0, but was $bufferSize!"
  )

  private val (_sink, switch, _done) =
    MergeHub
      .source[(Req, Respondee[Res])](bufferSize)
      .viaMat(KillSwitches.single)(Keep.both)
      .via(process)
      .toMat(Sink.foreach {
        case (response, respondee) => respondee ! Respondee.Response(response)
      }) { case ((sink, switch), done) => (sink, switch, done) }
      .withAttributes(ActorAttributes.supervisionStrategy(resume))
      .run()

  coordinatedShutdown(mat)
    .addTask(phase, s"shutdown-intoable-processor-$name") { () =>
      shutdown()
      whenDone
    }

  /**
    * Sink to be used with the `into` stream extension method locally.
    */
  def sink: ProcessSink[Req, Res] =
    _sink

  /**
    * Create a `SinkRef` to be used with the `into` stream extension method remotely, i.e. when this
    * processor is running on one member node of an Akka cluster and on another member node `into`
    * is used.
    */
  def sinkRef()(
      implicit mat: Materializer
  ): Future[SinkRef[(Req, Respondee[Res])]] =
    StreamRefs.sinkRef().to(_sink).run()

  /**
    * Shutdown this processor. Already accepted requests are completed, but no new ones are
    * accepted. To watch shutdown completion use [[whenDone]].
    */
  def shutdown(): Unit = {
    logger.warn(s"Shutdown for processor $name requested!")
    switch.shutdown()
  }

  /**
    * The returned `Future` is completed when the running process is completed, e.g. via
    * [[shutdown]] or unexpected failure.
    *
    * @return signal for completion
    */
  def whenDone: Future[Done] =
    _done

  private def resume(cause: Throwable) = {
    logger.error(s"Processor $name failed and resumes", cause)
    Supervision.Resume
  }
}
