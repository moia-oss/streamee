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

///*
// * Copyright 2018 MOIA GmbH
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package io.moia.streamee
//
//import akka.NotUsed
//import akka.actor.Scheduler
//import akka.actor.typed.ActorRef
//import akka.actor.typed.scaladsl.AskPattern.Askable
//import akka.stream.scaladsl.{ Flow, FlowOps, Source }
//import akka.stream.{ Materializer, SinkRef }
//import akka.util.Timeout
//import scala.concurrent.duration.FiniteDuration
//import scala.concurrent.{ ExecutionContext, Future, Promise }
//
//package object intoable {
//
//  type IntoableFlow[A, B] = Flow[(A, Respondee[B]), (B, Respondee[B]), NotUsed]
//
//  type IntoableSinkRef[A, B] = SinkRef[(A, Respondee[B])]
//
//  type Respondee[A] = ActorRef[Respondee.Command[A]]
//
//  type RespondeeFactory[A] = ActorRef[RespondeeFactory.Command[A]]
//
//  /**
//    * Extension methods for `Source`s.
//    */
//  implicit final class SourceOps[O, M](val source: Source[O, M]) extends AnyVal {
//
//    /**
//      * Attaches to an "intoable" process managed by a [[IntoableRunner]].
//      */
//    def into[O2](
//        sinkRefFor: O => Future[IntoableSinkRef[O, O2]],
//        responseTimeout: FiniteDuration,
//        parallelism: Int = 1024
//    )(implicit mat: Materializer,
//      ec: ExecutionContext,
//      scheduler: Scheduler,
//      respondeeFactory: ActorRef[RespondeeFactory.Command[O2]]): Source[O2, M] =
//      intoImpl(source, sinkRefFor, responseTimeout, parallelism)
//  }
//
//  /**
//    * Extension methods for `Flow`s.
//    */
//  implicit final class FlowExt[I, O, M](val flow: Flow[I, O, M]) extends AnyVal { // Name FlowOps is taken by Akka!
//
//    def into[O2](
//        sinkRefFor: O => Future[IntoableSinkRef[O, O2]],
//        responseTimeout: FiniteDuration,
//        parallelism: Int = 1024
//    )(implicit mat: Materializer,
//      ec: ExecutionContext,
//      scheduler: Scheduler,
//      respondeeFactory: ActorRef[RespondeeFactory.Command[O2]]): Flow[I, O2, M] =
//      intoImpl(flow, sinkRefFor, responseTimeout, parallelism)
//  }
//
//  private def intoImpl[O, O2, M](
//      flowOps: FlowOps[O, M],
//      sinkRefFor: O => Future[IntoableSinkRef[O, O2]],
//      responseTimeout: FiniteDuration,
//      parallelism: Int
//  )(implicit mat: Materializer,
//    ec: ExecutionContext,
//    scheduler: Scheduler,
//    respondeeFactory: ActorRef[RespondeeFactory.Command[O2]]): flowOps.Repr[O2] =
//    flowOps
//      .mapAsyncUnordered(parallelism) { request =>
//        val response = Promise[O2]()
//        val respondee = {
//          implicit val timeout: Timeout = responseTimeout // Safe to use, because of fast local ask
//          val created =
//          respondeeFactory ? { replyTo: ActorRef[RespondeeFactory.RespondeeCreated[O2]] =>
//            RespondeeFactory.CreateRespondee[O2](response, responseTimeout, replyTo)
//          }
//          created.map(x => x.respondee)
//        }
//        val sinkRef = {
//          val deadline = responseTimeout.fromNow
//          sinkRefFor(request)
//            .recoverWith {
//              case _ if deadline.hasTimeLeft => sinkRefFor(request)
//              case cause                     => Future.failed(cause)
//            }
//        }
//        Future.successful((request, response)).zip(sinkRef.zip(respondee))
//      }
//      .mapAsyncUnordered(parallelism) {
//        case ((request, response), (sinkRef, respondee)) =>
//          Source.single((request, respondee)).runWith(sinkRef)
//          response.future
//      }
//
//}
