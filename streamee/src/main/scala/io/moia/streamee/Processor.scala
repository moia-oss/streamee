/*
 * Copyright (c) MOIA GmbH
 */

package io.moia.streamee

import akka.Done
import akka.stream.scaladsl.{
  Broadcast,
  Flow,
  GraphDSL,
  Keep,
  Sink,
  Source,
  SourceQueue,
  Unzip,
  Zip
}
import akka.stream.stage.{ GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler }
import akka.stream.{
  ActorAttributes,
  Attributes,
  FanInShape2,
  FlowShape,
  Inlet,
  Materializer,
  Outlet,
  OverflowStrategy,
  Supervision
}
import org.apache.logging.log4j.scala.Logging
import scala.concurrent.{ ExecutionContext, Future, Promise }

/**
  * Runs a pipeline (Akka Streams flow) for processing commands to results. See [[Processor.apply]]
  * for details.
  */
object Processor extends Logging {

  /**
    * Switch to shut down the related processor. See [[ShutdownSwitch.shutdown]] for details.
    */
  sealed trait ShutdownSwitch {

    /**
      * Shuts down the related processor: all commands are dropped and once all in-flight commands
      * have been processed, the returned future is completed successfully.
      * @return future completed successfully once all in-flight commands have been processed
      */
    def shutdown(): Future[Done]
  }

  private final class ShutdownSwitchStage[A](implicit ec: ExecutionContext)
      extends GraphStageWithMaterializedValue[FanInShape2[A, Done, A], ShutdownSwitch] {

    override val shape: FanInShape2[A, Done, A] =
      new FanInShape2(Inlet[A]("in0"), Inlet[Done]("in1"), Outlet[A]("out"))

    override def createLogicAndMaterializedValue(
        as: Attributes
    ): (GraphStageLogic, ShutdownSwitch) = {
      val logic = new ShutdownSwitchStageLogic(shape)
      (logic, logic.shutdownSwitch)
    }
  }

  private final class ShutdownSwitchStageLogic[A](shape: FanInShape2[A, Done, A])(
      implicit ec: ExecutionContext
  ) extends GraphStageLogic(shape)
      with InHandler
      with OutHandler {
    import shape._

    private val shutdownTriggered = Promise[Done]()

    private val shutdownCompleted = Promise[Done]()

    val shutdownSwitch: ShutdownSwitch =
      new ShutdownSwitch {
        override def shutdown(): Future[Done] = {
          logger.info("Shutting down processor")
          shutdownTriggered.trySuccess(Done)
          shutdownCompleted.future
        }
      }

    private var inFlightCount = 0

    setHandler(out, this)
    setHandler(in0, this)
    setHandler(
      in1,
      new InHandler {
        override def onPush(): Unit = {
          grab(in1)
          pull(in1)
          inFlightCount -= 1
        }
      }
    )

    override def preStart(): Unit = {
      super.preStart()
      pull(in1)
      val callback = getAsyncCallback(shutdown)
      shutdownTriggered.future.foreach(callback.invoke)
    }

    override def onPush(): Unit = {
      push(out, grab(in0))
      inFlightCount += 1
    }

    override def onPull(): Unit =
      pull(in0)

    private def shutdown(done: Done) = {
      if (inFlightCount <= 0) shutdownCompleted.trySuccess(Done)
      setHandler(in0, new InHandler { override def onPush(): Unit = () })
      setHandler(
        in1,
        new InHandler {
          override def onPush(): Unit = {
            grab(in1)
            if (!hasBeenPulled(in1)) pull(in1)
            inFlightCount -= 1
            if (inFlightCount <= 0) shutdownCompleted.trySuccess(Done)
          }
        }
      )
      setHandler(out, new OutHandler { override def onPull(): Unit = () })
    }
  }

  /**
    * Runs a pipeline (Akka Streams flow) for processing commands to results. Commands offered via
    * the returned queue are emitted into the given `logic` flow. Once that emits a result, the
    * given promise is completed with success. If the logic back-pressures, offered commands are
    * dropped. The returned [[ShutdownSwitch]] can be used to shutdown: all commands are dropped and
    * once all in-flight commands have been processed, the future returned from
    * [[ShutdownSwitch.shutdown]] is completed successfully.
    *
    * '''Attention''': the given domain logic pipeline must emit exactly one result for every
    * command and the sequence of the elements in the stream must be maintained!
    *
    * @param logic domain logic pipeline from command to result
    * @param mat materializer to run the stream
    * @tparam C command type
    * @tparam R result type
    * @return queue for command-promise pairs and shutdown switch
    */
  // TODO Maybe require C and R to have a correlation id, e.g. via `<: Correlated` or `: Correlated`?
  def apply[C, R](
      logic: Flow[C, R, Any]
  )(implicit mat: Materializer): (SourceQueue[(C, Promise[R])], ShutdownSwitch) = {
    import mat.executionContext
    Source
      .queue[(C, Promise[R])](0, OverflowStrategy.dropNew) // Important: use 0 to disable buffer!
      .viaMat(wrapLogic(logic))(Keep.both)
      .to(Sink.foreach { case (r, promisedR) => promisedR.trySuccess(r) })
      .withAttributes(ActorAttributes.supervisionStrategy(_ => Supervision.Resume)) // The stream must not die!
      .run()
  }

  private def wrapLogic[C, R](logic: Flow[C, R, Any])(implicit ec: ExecutionContext) = {
    val controllerStage = new ShutdownSwitchStage[(C, Promise[R])]
    Flow.fromGraph(GraphDSL.create(controllerStage) { implicit builder => shutdownSwitchStage =>
      import GraphDSL.Implicits._
      val unzip  = builder.add(Unzip[C, Promise[R]]())
      val zip    = builder.add(Zip[R, Promise[R]]())
      val buf    = builder.add(Flow[Promise[R]].buffer(42, OverflowStrategy.backpressure)) // TODO Use configurable value!
      val bcast  = builder.add(Broadcast[(R, Promise[R])](2))
      val toDone = builder.add(Flow[Any].map(_ => Done))

      // format: OFF
      shutdownSwitchStage.out ~> unzip.in
                                 unzip.out0 ~>  logic ~> zip.in0
                                 unzip.out1 ~>   buf  ~> zip.in1
                                                         zip.out ~> bcast.in
      shutdownSwitchStage.in1               <~ toDone <~            bcast.out(1)
      // format: ON

      FlowShape(shutdownSwitchStage.in0, bcast.out(0))
    })
  }
}
