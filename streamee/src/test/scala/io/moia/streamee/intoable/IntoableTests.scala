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
package intoable

import akka.stream.scaladsl.{ Sink, Source }
import akka.stream.testkit.scaladsl.TestSource
import scala.concurrent.{ Future, Promise }
import scala.concurrent.duration.DurationInt
import utest._

object IntoableTests extends ActorTestSuite {
  import testKit._

  private implicit val respondeeFactory: RespondeeFactory[Int] =
    spawn(RespondeeFactory[Int]())

  override def tests: Tests =
    Tests {
      'intoable - {
        val (intoableSink, _) = runProcess(Process[Int, Int]().map(_ + 1), 1)
        val process           = Process[Int, Int]().into(intoableSink, 42)
        val result1 =
          Source(0.to(9))
            .startContextPropagation(_ => Promise[Int]())
            .via(process)
            .endContextPropagation
            .map(_._1)
            .runWith(Sink.seq)
            .map { result =>
              assert(result == 1.to(10))
            }
        val result2 =
          Source(10.to(19))
            .startContextPropagation(_ => Promise[Int]())
            .via(process)
            .endContextPropagation
            .map(_._1)
            .runWith(Sink.seq)
            .map { result =>
              assert(result == 11.to(20))
            }
        Future.sequence(List(result1, result2))
      }

      'remotelyIntoable - {
        val (intoableSink, _, _) =
          runRemotelyIntoableProcess(RemotelyIntoableProcess[Int, Int]().map(_ + 1), 1)
        val process = Process[Int, Int]().into(intoableSink, 1.second, 42)
        val result1 =
          Source(0.to(9))
            .startContextPropagation(_ => Promise[Int]())
            .via(process)
            .endContextPropagation
            .map(_._1)
            .runWith(Sink.seq)
            .map { result =>
              assert(result == 1.to(10))
            }
        val result2 =
          Source(10.to(19))
            .startContextPropagation(_ => Promise[Int]())
            .via(process)
            .endContextPropagation
            .map(_._1)
            .runWith(Sink.seq)
            .map { result =>
              assert(result == 11.to(20))
            }
        Future.sequence(List(result1, result2))
      }

      'remotelyIntoableSwitch - {
        val (intoableSink, switch, _) =
          runRemotelyIntoableProcess(RemotelyIntoableProcess[Int, Int](), 1)
        val process = Process[Int, Int]().into(intoableSink, 1.second, 42)
        val publisher =
          TestSource
            .probe[Int]
            .startContextPropagation(_ => Promise[Int]())
            .via(process)
            .endContextPropagation
            .to(Sink.ignore)
            .run()
        switch.shutdown()
        publisher.expectCancellation()
      }
    }
}
