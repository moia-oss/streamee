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
package demo

import akka.stream.{ DelayOverflowStrategy, Materializer, SinkRef }
import akka.stream.scaladsl.Flow
import akka.NotUsed
import akka.actor.{ CoordinatedShutdown, Scheduler }
import akka.actor.typed.ActorRef
import akka.cluster.sharding.typed.scaladsl.{ ClusterSharding, EntityTypeKey, ShardedEntity }
import akka.util.Timeout
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.{ DurationInt, FiniteDuration }

/**
  * A trivial domain logic process demoing the use of `into`.
  */
object Length {

  type Process = Flow[String, String, NotUsed]

  final case class Config(retryTimeout: FiniteDuration)

  def apply(
      config: Config,
      delayedLengthFor: String => Future[SinkRef[(String, Respondee[Int])]]
  )(implicit mat: Materializer,
    ec: ExecutionContext,
    scheduler: Scheduler,
    respondeeFactory: ActorRef[RespondeeFactory.Command[Int]]): Flow[String, String, NotUsed] = {
    import config._
    Flow[String]
      .into(s => delayedLengthFor(s), 42, retryTimeout)
      .map(_.toString)
  }
}

/**
  * A trivial "intoable" process.
  */
object DelayedLength {

  def apply(): Flow[(String, Respondee[Int]), (Int, Respondee[Int]), NotUsed] =
    Flow[(String, Respondee[Int])]
      .delay(4.seconds, DelayOverflowStrategy.backpressure)
      .map { case (s, p) => (s.length, p) }
}

/**
  * IntoableRunners for [[DelayedLength]] managed by Akka Cluster Sharding.
  */
object DelayedLengthSharding {

  final case class Config(askTimeout: FiniteDuration)

  val entityKey: EntityTypeKey[IntoableRunner.Command] =
    EntityTypeKey[IntoableRunner.Command]("delayed-length-intoable-runner")

  def apply[A, B](
      config: Config,
      process: Flow[(A, Respondee[B]), (B, Respondee[B]), NotUsed],
      sharding: ClusterSharding,
      shutdown: CoordinatedShutdown
  )(implicit mat: Materializer): String => Future[SinkRef[(String, Respondee[Int])]] = {
    val entity =
      ShardedEntity(_ => IntoableRunner(process, shutdown), entityKey, IntoableRunner.Shutdown)
    sharding.start(entity)

    implicit val timeout: Timeout = config.askTimeout
    sharding.entityRefFor(entityKey, _).ask(IntoableRunner.GetSinkRef.apply)
  }
}
