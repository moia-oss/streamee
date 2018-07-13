/*
 * Copyright (c) MOIA GmbH
 */

package io.moia.streamee

import akka.actor.CoordinatedShutdown
import akka.actor.CoordinatedShutdown.PhaseServiceRequestsDone
import akka.stream.scaladsl.{ Flow, GraphDSL, Keep, Sink, Source, Unzip, Zip }
import akka.stream.{ ActorAttributes, FlowShape, Materializer, OverflowStrategy, Supervision }
import org.apache.logging.log4j.scala.Logging
import scala.concurrent.Promise

/**
  * Runs a domain logic pipeline (Akka Streams flow) for processing commands to results. See
  * [[Processor.apply]] for details.
  */
object Processor extends Logging {

  /**
    * Runs a domain logic pipeline (Akka Streams flow) for processing commands to results. Commands
    * offered via the returned queue are emitted into the given `pipeline`. Once results are
    * available the promise given together with the command is completed with success. If the
    * pipeline back-pressures, offered commands are dropped.
    *
    * A task is registered with Akka Coordinated Shutdown in the "service-requests-done" phase to
    * ensure that no more commands are accepted and all in-flight commands have been processed
    * before continuing with the shutdown.
    *
    * '''Attention''': the given domain logic pipeline must emit exactly one result for every
    * command and the sequence of the elements in the pipeline must be maintained!
    *
    * @param pipeline domain logic pipeline from command to result
    * @param parallelsim maximum number of in-flight commands
    * @param shutdown Akka Coordinated Shutdown
    * @param mat materializer to run the stream
    * @tparam C command type
    * @tparam R result type
    * @return queue for command-promise pairs
    */
  def apply[C, R](pipeline: Flow[C, R, Any], parallelsim: Int, shutdown: CoordinatedShutdown)(
      implicit mat: Materializer
  ): Processor[C, R] = {
    val (sourceQueueWithComplete, done) =
      Source
        .queue[(C, Promise[R])](1, OverflowStrategy.dropNew) // Must be 1: for 0 offers could "hang", for larger values completing would not work, see: https://github.com/akka/akka/issues/25349
        .via(bypassPromise(pipeline, parallelsim))
        .toMat(Sink.foreach { case (r, promisedR) => promisedR.trySuccess(r) })(Keep.both)
        .withAttributes(ActorAttributes.supervisionStrategy(_ => Supervision.Resume)) // The stream must not die!
        .run()
    shutdown.addTask(PhaseServiceRequestsDone, "processor") { () =>
      sourceQueueWithComplete.complete()
      done // `sourceQueueWithComplete.watchCompletion` seems broken, hence use `done`
    }
    sourceQueueWithComplete
  }

  private def bypassPromise[C, R](pipeline: Flow[C, R, Any], parallelsim: Int) =
    Flow.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._
      val unzip  = builder.add(Unzip[C, Promise[R]]())
      val zip    = builder.add(Zip[R, Promise[R]]())
      val buffer = builder.add(Flow[Promise[R]].buffer(parallelsim, OverflowStrategy.backpressure))

      // format: OFF
      unzip.out0 ~> pipeline ~> zip.in0
      unzip.out1 ~>  buffer  ~> zip.in1
      // format: ON

      FlowShape(unzip.in, zip.out)
    })
}
