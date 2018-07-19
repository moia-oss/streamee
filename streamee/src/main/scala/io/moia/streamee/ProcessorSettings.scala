package io.moia.streamee

import akka.actor.{ ActorSystem => UntypedActorSystem }
import akka.actor.typed.{ ActorSystem, Extension, ExtensionId }

/**
  * Settings for processors. Can be accessed from untyped and typed Akka code.
  */
object ProcessorSettings extends ExtensionId[ProcessorSettings] {

  /**
    * Lookup or create an instance of the processor settings extension.
    */
  def apply(untypedSystem: UntypedActorSystem): ProcessorSettings = {
    import akka.actor.typed.scaladsl.adapter._
    apply(untypedSystem.toTyped)
  }

  override def createExtension(system: ActorSystem[_]): ProcessorSettings =
    new ProcessorSettingsExtension(system)
}

/**
  * Settings for processors.
  */
sealed trait ProcessorSettings extends Extension {
  this: ProcessorSettingsExtension =>

  /**
    * Buffer size of the processor queue. Must be positive!
    *
    * Usually a buffer larger than one should not be needed, if the wrapped domain logic pipeline
    * offers sufficient parallelism.
    *
    * ATTENTNION: Currently must be 1, see https://github.com/akka/akka/issues/25349!
    */
  final val bufferSize: Int = {
    val bufferSize = system.settings.config.getInt("streamee.processor.buffer-size")
    if (bufferSize <= 0)
      throw new IllegalArgumentException("streamee.processor.buffer-size must be positive!")
    if (bufferSize != 1)
      throw new IllegalArgumentException(
        "streamee.processor.buffer-size currently must be 1, see docs in reference.conf!"
      )
    bufferSize
  }

  /**
    * The maximum number of commands which can be in-flight in the wrapped domain logic pipeline.
    *
    * Large values should not be an issue, because for each command in-flight there is just a
    * buffered promise (which is rather lightweight).
    *
    * Must be positive!
    */
  final val maxNrOfInFlightCommands: Int = {
    val maxNrOfInFlightCommands =
      system.settings.config.getInt("streamee.processor.max-nr-of-in-flight-commands")
    if (maxNrOfInFlightCommands <= 0)
      throw new IllegalArgumentException(
        "streamee.processor.max-nr-of-in-flight-commands must be positive!"
      )
    maxNrOfInFlightCommands
  }
}

private[streamee] final class ProcessorSettingsExtension(val system: ActorSystem[_])
    extends ProcessorSettings
