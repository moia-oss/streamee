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

package io.moia.streamee.demo

import akka.actor.Scheduler
import akka.stream.Materializer
import io.moia.streamee.{ Handler, Process }
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext

object WordShufflerHandler {
  import WordShuffler._

  final case class Config(timeout: FiniteDuration,
                          bufferSize: Int,
                          wordShuffler: WordShuffler.Config)

  def apply(config: Config)(implicit mat: Materializer,
                            ec: ExecutionContext,
                            scheduler: Scheduler): Handler[ShuffleWord, WordShuffled] = {
    import config._
    Process.runToHandler(WordShuffler(config.wordShuffler), timeout, bufferSize, "word-shuffler")
  }
}
