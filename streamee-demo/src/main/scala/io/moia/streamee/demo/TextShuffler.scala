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

import akka.stream.{ Attributes, DelayOverflowStrategy }
import io.moia.streamee.{ FlowWithContextExt2, Process }
import scala.collection.immutable.Seq
import scala.concurrent.duration.FiniteDuration

object TextShuffler {

  final case class ShuffleText(text: String)
  final case class TextShuffled(originalText: String, shuffledText: String)

  final case class Config(delay: FiniteDuration)

  def apply(config: Config): Process[ShuffleText, TextShuffled, TextShuffled] =
    delay(config.delay)
      .via(keepOriginalAndSplit)
      .via(shuffleWords)
      .via(concat)

  def delay(of: FiniteDuration): Process[ShuffleText, ShuffleText, TextShuffled] =
    Process[ShuffleText, TextShuffled]() // Type annotation only needed by IDEA!
      .delay(of, DelayOverflowStrategy.backpressure)
      .withAttributes(Attributes.inputBuffer(1, 1))

  def keepOriginalAndSplit: Process[ShuffleText, (String, Seq[String]), TextShuffled] =
    Process[ShuffleText, TextShuffled]() // Here the type annotation is mandatory!
      .map(_.text)
      .pushIn
      .map(_.split(" ").toList)
      .popIn // Current limitation: input must be popped from context before terminating the stage in order to allow for a `Process` return type!

  def shuffleWords: Process[(String, Seq[String]), (String, Seq[String]), TextShuffled] =
    Process()
      .map { case (originalText, words) => (originalText, words.map(WordShuffler.shuffleWord)) }

  def concat: Process[(String, Seq[String]), TextShuffled, TextShuffled] =
    Process()
      .map {
        case (originalText, shuffledWords) =>
          TextShuffled(originalText, shuffledWords.mkString(" "))
      }
}
