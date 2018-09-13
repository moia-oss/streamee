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

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import com.typesafe.config.ConfigFactory
import utest._

object ProcessorSettingsTests extends TestSuite {

  override def tests: Tests =
    Tests {
      'throwNonPositiveBufferSize - {
        val config =
          ConfigFactory
            .parseString("streamee.processor.buffer-size=0")
            .withFallback(ConfigFactory.load())
        val system = ActorSystem[Nothing](Behaviors.empty, getClass.getSimpleName.init, config)
        try intercept[IllegalArgumentException](ProcessorSettings(system))
        finally system.terminate()
      }

      'throwNotOneBufferSize - {
        val config =
          ConfigFactory
            .parseString("streamee.processor.buffer-size=2")
            .withFallback(ConfigFactory.load())
        val system = ActorSystem[Nothing](Behaviors.empty, getClass.getSimpleName.init, config)
        try intercept[IllegalArgumentException](ProcessorSettings(system))
        finally system.terminate()
      }

      'bufferSize - {
        val config =
          ConfigFactory
            .parseString("streamee.processor.buffer-size=1")
            .withFallback(ConfigFactory.load())
        val system = ActorSystem[Nothing](Behaviors.empty, getClass.getSimpleName.init, config)
        try {
          val bufferSize = ProcessorSettings(system).bufferSize
          assert(bufferSize == 1)
        } finally system.terminate()
      }

      'throwNonPositiveMaxNrOfInFlightRequests - {
        val config =
          ConfigFactory
            .parseString("streamee.processor.max-nr-of-in-flight-requests=0")
            .withFallback(ConfigFactory.load())
        val system = ActorSystem[Nothing](Behaviors.empty, getClass.getSimpleName.init, config)
        try intercept[IllegalArgumentException](ProcessorSettings(system))
        finally system.terminate()
      }

      'maxNrOfInFlightRequests - {
        val config =
          ConfigFactory
            .parseString("streamee.processor.max-nr-of-in-flight-requests=1024")
            .withFallback(ConfigFactory.load())
        val system = ActorSystem[Nothing](Behaviors.empty, getClass.getSimpleName.init, config)
        try {
          val maxNrOfInFlightRequests = ProcessorSettings(system).maxNrOfInFlightRequests
          assert(maxNrOfInFlightRequests == 1024)
        } finally system.terminate()
      }
    }
}
