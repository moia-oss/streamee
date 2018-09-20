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

import akka.actor.{ ActorSystem => UntypedActorSystem }
import akka.actor.typed.{ ActorSystem, Extension, ExtensionId }
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{ Duration, FiniteDuration }

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

  /**
    * Buffer size of the processor queue. Must be positive!
    *
    * Usually a buffer larger than one should not be needed, if the wrapped domain logic process
    * offers sufficient parallelism.
    *
    * ATTENTNION: Currently must be 1, see https://github.com/akka/akka/issues/25349!
    */
  def bufferSize: Int

  /**
    * The interval at which promised responses that have completed are removed from the internal
    * processor logic. Must be positive!
    *
    * Should be roughly the same duration like the expiry timeout of the promises.
    */
  def sweepCompleteResponsesInterval: FiniteDuration
}

/**
  * Settings for processors. Mainly useful for testing.
  */
final case class PlainProcessorSettings(bufferSize: Int,
                                        sweepCompleteResponsesInterval: FiniteDuration)
    extends ProcessorSettings {

  require(bufferSize > 0, "streamee.processor.buffer-size must be positive!")
  require(bufferSize == 1, "streamee.processor.buffer-size currently must be 1!")
  require(sweepCompleteResponsesInterval > Duration.Zero,
          "streamee.processor.sweep-complete-responses-interval must be positive!")
}

/**
  * Settings for processors.
  */
private final class ProcessorSettingsExtension(val system: ActorSystem[_])
    extends ProcessorSettings
    with Extension {
  //this: ProcessorSettings =>

  val bufferSize: Int = {
    val bufferSize = system.settings.config.getInt("streamee.processor.buffer-size")
    require(bufferSize > 0, "streamee.processor.buffer-size must be positive!")
    require(bufferSize == 1, "streamee.processor.buffer-size currently must be 1!")
    bufferSize
  }

  val sweepCompleteResponsesInterval: FiniteDuration = {
    val duration =
      system.settings.config.getDuration("streamee.processor.sweep-complete-responses-interval")
    val sweepPromisesInterval = FiniteDuration(duration.toNanos, TimeUnit.NANOSECONDS)
    require(sweepPromisesInterval > Duration.Zero,
            "streamee.processor.sweep-complete-responses-interval must be positive!")
    sweepPromisesInterval
  }
}
