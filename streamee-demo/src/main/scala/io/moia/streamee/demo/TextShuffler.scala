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
import akka.stream.{ Attributes, DelayOverflowStrategy, Materializer }
import akka.stream.scaladsl.{ Sink, Source }
import io.moia.streamee.{ FlowWithContextExt2, Process, Processor, SourceExt }
import scala.collection.immutable.Seq
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext

object TextShuffler {

  final case class ShuffleText(text: String)
  final case class TextShuffled(originalText: String, shuffledText: String)

  final case class Config(delay: FiniteDuration, wordShufflerTimeout: FiniteDuration)

  def apply(config: Config,
            wordShufflerProcessor: Processor[WordShuffler.ShuffleWord, WordShuffler.WordShuffled])(
      implicit mat: Materializer,
      ec: ExecutionContext,
      scheduler: Scheduler
  ): Process[ShuffleText, TextShuffled, TextShuffled] =
    delay(config.delay)
      .via(keepOriginalAndSplit)
      .via(shuffleWords2(wordShufflerProcessor))
      .via(concat)

  def delay(of: FiniteDuration): Process[ShuffleText, ShuffleText, TextShuffled] =
    Process[ShuffleText, TextShuffled]() // Type annotation only needed by IDEA!
      .delay(of, DelayOverflowStrategy.backpressure)
      .withAttributes(Attributes.inputBuffer(1, 1))

  def keepOriginalAndSplit: Process[ShuffleText, (String, Seq[String]), TextShuffled] =
    Process[ShuffleText, TextShuffled]() // Here the type annotation is mandatory!
      .map(_.text)
      .push // push the original text
      .map(_.split(" ").toList)
      .pop // pop the original text

  def shuffleWords: Process[(String, Seq[String]), (String, Seq[String]), TextShuffled] =
    Process()
      .map { case (originalText, words) => (originalText, words.map(WordShuffler.shuffleWord)) }

  def shuffleWords2(
      wordShufflerProcessor: Processor[WordShuffler.ShuffleWord, WordShuffler.WordShuffled]
  )(
      implicit mat: Materializer
  ): Process[(String, Seq[String]), (String, Seq[String]), TextShuffled] =
    Process[(String, Seq[String]), TextShuffled]()
      .push(_._1, _._2) // push the original text
      .mapAsync(1) { words =>
        Source(words)
          .map(WordShuffler.ShuffleWord)
          .into(wordShufflerProcessor)
          .runWith(Sink.seq)
      }
      .map(_.map(_.word))
      .pop // pop the original text

  def concat: Process[(String, Seq[String]), TextShuffled, TextShuffled] =
    Process()
      .map {
        case (originalText, shuffledWords) =>
          TextShuffled(originalText, shuffledWords.mkString(" "))
      }
}
