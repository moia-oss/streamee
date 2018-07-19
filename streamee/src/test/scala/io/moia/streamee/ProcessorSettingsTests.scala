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

      'throwNonPositiveMaxNrOfInFlightCommands - {
        val config =
          ConfigFactory
            .parseString("streamee.processor.max-nr-of-in-flight-commands=0")
            .withFallback(ConfigFactory.load())
        val system = ActorSystem[Nothing](Behaviors.empty, getClass.getSimpleName.init, config)
        try intercept[IllegalArgumentException](ProcessorSettings(system))
        finally system.terminate()
      }

      'maxNrOfInFlightCommands - {
        val config =
          ConfigFactory
            .parseString("streamee.processor.max-nr-of-in-flight-commands=1024")
            .withFallback(ConfigFactory.load())
        val system = ActorSystem[Nothing](Behaviors.empty, getClass.getSimpleName.init, config)
        try {
          val maxNrOfInFlightCommands = ProcessorSettings(system).maxNrOfInFlightCommands
          assert(maxNrOfInFlightCommands == 1024)
        } finally system.terminate()
      }
    }
}
