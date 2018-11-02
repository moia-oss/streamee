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

package io.moia

import akka.actor.typed.ActorRef
import akka.cluster.sharding.typed.scaladsl.EntityRef
import akka.stream.{ Materializer, SinkRef }
import akka.stream.scaladsl.{ Flow, FlowOps, Source }
import akka.util.Timeout
import scala.concurrent.{ Future, Promise }

package object streamee {

  /**
    * Extension methods for `Source`s.
    */
  implicit final class SourceOps[O, M](val source: Source[O, M]) extends AnyVal {

    def into[O2](entityRef: O => EntityRef[IntoableRunner.GetSinkRef[O, O2]],
                 parallelism: Int)(implicit ec: Materializer, timeout: Timeout): Source[O2, M] =
      intoImpl(source, entityRef, parallelism)
  }

  /**
    * Extension methods for `Flow`s.
    */
  implicit final class FlowExt[I, O, M](val flow: Flow[I, O, M]) extends AnyVal { // Name FlowOps is taken by Akka!

    def into[O2](entityRef: O => EntityRef[IntoableRunner.GetSinkRef[O, O2]],
                 parallelism: Int)(implicit ec: Materializer, timeout: Timeout): Flow[I, O2, M] =
      intoImpl(flow, entityRef, parallelism)
  }

  private def intoImpl[O, O2, M](
      flowOps: FlowOps[O, M],
      entityRef: O => EntityRef[IntoableRunner.GetSinkRef[O, O2]],
      parallelism: Int
  )(implicit mat: Materializer, askTimeout: Timeout): flowOps.Repr[O2] =
    flowOps
      .mapAsync(parallelism) { o =>
        // TODO Retry!
        val sinkRef =
          entityRef(o).ask(
            (replyTo: ActorRef[SinkRef[(O, Promise[O2])]]) => IntoableRunner.GetSinkRef(replyTo)
          )
        Future.successful(o).zip(sinkRef)
      }
      .mapAsync(parallelism) {
        case (n, sinkRef) =>
          val p = Promise[O2]()
          Source.single((n, p)).runWith(sinkRef)
          p.future
      }
}
