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

import akka.stream.{ Materializer, SinkRef }
import akka.stream.scaladsl.{ Flow, FlowOps, Source }
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration.FiniteDuration

package object streamee {

  /**
    * Extension methods for `Source`s.
    */
  implicit final class SourceOps[O, M](val source: Source[O, M]) extends AnyVal {

    /**
      * Attaches to an "intoable" process managed by a [[IntoableRunner]].
      */
    def into[O2](
        sinkRefFor: O => Future[SinkRef[(O, Promise[O2])]],
        parallelism: Int,
        retryTimeout: FiniteDuration
    )(implicit mat: Materializer, ec: ExecutionContext): Source[O2, M] =
      intoImpl(source, sinkRefFor, parallelism, retryTimeout)
  }

  /**
    * Extension methods for `Flow`s.
    */
  implicit final class FlowExt[I, O, M](val flow: Flow[I, O, M]) extends AnyVal { // Name FlowOps is taken by Akka!

    def into[O2](
        sinkRefFor: O => Future[SinkRef[(O, Promise[O2])]],
        parallelism: Int,
        retryTimeout: FiniteDuration
    )(implicit mat: Materializer, ec: ExecutionContext): Flow[I, O2, M] =
      intoImpl(flow, sinkRefFor, parallelism, retryTimeout)
  }

  private def intoImpl[O, O2, M](
      flowOps: FlowOps[O, M],
      sinkRefFor: O => Future[SinkRef[(O, Promise[O2])]],
      parallelism: Int,
      retryTimeout: FiniteDuration
  )(implicit mat: Materializer, ec: ExecutionContext): flowOps.Repr[O2] =
    flowOps
      .mapAsync(parallelism) { o =>
        val deadline = retryTimeout.fromNow
        val sinkRef =
          sinkRefFor(o)
            .recoverWith {
              case _ if deadline.hasTimeLeft => sinkRefFor(o)
              case cause                     => Future.failed(cause)
            }
        Future.successful(o).zip(sinkRef)
      }
      .mapAsync(parallelism) {
        case (n, sinkRef) =>
          val p = Promise[O2]()
          Source.single((n, p)).runWith(sinkRef)
          p.future
      }
}
