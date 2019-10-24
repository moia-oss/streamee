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

import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.{ ActorRef, Scheduler }
import akka.stream.{ Attributes, DelayOverflowStrategy, Materializer }
import akka.stream.scaladsl.{ RestartSink, Sink, Source }
import io.moia.streamee.{
  FlowWithPairedContextOps,
  Process,
  ProcessSink,
  ProcessSinkRef,
  SourceExt
}
import io.moia.streamee.demo.WordShuffler.{ ShuffleWord, WordShuffled }
import scala.collection.immutable.Seq
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration.FiniteDuration

object TextShuffler {

  final case class ShuffleText(text: String)
  final case class TextShuffled(originalText: String, shuffledText: String)

  final case class Config(
      delay: FiniteDuration,
      wordShufflerProcessorTimeout: FiniteDuration,
      wordShufflerAskTimeout: FiniteDuration
  )

  def apply(config: Config, wordShufflerRunner: ActorRef[WordShufflerRunner.Command])(
      implicit mat: Materializer,
      ec: ExecutionContext,
      scheduler: Scheduler
  ): Process[ShuffleText, TextShuffled, TextShuffled] = {
    import config._

    def wordShufflerSinkRef(): Future[ProcessSinkRef[ShuffleWord, WordShuffled]] =
      wordShufflerRunner
        .ask { replyTo: ActorRef[ProcessSinkRef[ShuffleWord, WordShuffled]] =>
          WordShufflerRunner.GetProcessSinkRef(replyTo)
        }(wordShufflerAskTimeout, scheduler)
        .recoverWith { case _ => wordShufflerSinkRef() }

    val wordShufflerSink =
      RestartSink.withBackoff(wordShufflerAskTimeout, wordShufflerAskTimeout, 0) { () =>
        Await.result(wordShufflerSinkRef().map(_.sink), wordShufflerAskTimeout) // Hopefully we can get rid of blocking soon: https://github.com/akka/akka/issues/25934
      }

    delayRequest(delay)
      .via(keepOriginalAndSplit)
      .via(shuffleWords(wordShufflerSink, wordShufflerProcessorTimeout))
      .via(concat)
  }

  def delayRequest(of: FiniteDuration): Process[ShuffleText, ShuffleText, TextShuffled] =
    Process()
      .delay(of, DelayOverflowStrategy.backpressure)
      .addAttributes(Attributes.inputBuffer(1, 1))

  def keepOriginalAndSplit: Process[ShuffleText, (String, Seq[String]), TextShuffled] =
    Process[ShuffleText, TextShuffled]() // Here the type annotation is mandatory!
      .map(_.text)
      .push // push the original text
      .map(_.split(" ").toList)
      .pop // pop the original text

  def shuffleWords(
      wordShufflerSink: ProcessSink[WordShuffler.ShuffleWord, WordShuffler.WordShuffled],
      wordShufflerProcessorTimeout: FiniteDuration
  )(
      implicit mat: Materializer
  ): Process[(String, Seq[String]), (String, Seq[String]), TextShuffled] =
    Process[(String, Seq[String]), TextShuffled]()
      .push(_._1, _._2) // push the original text, because it gets lost (shuffled) when ingesting into the `wordShufflerSink`
      .mapAsync(1) { words =>
        Source(words)
          .map(WordShuffler.ShuffleWord)
          .into(wordShufflerSink, wordShufflerProcessorTimeout)
          .runWith(Sink.seq)
      }
      .map(_.map(_.word))
      .pop // pop the original text, because we need it togehter with the shuffled for the purpose of displaying

  def concat: Process[(String, Seq[String]), TextShuffled, TextShuffled] =
    Process().map {
      case (originalText, shuffledWords) => TextShuffled(originalText, shuffledWords.mkString(" "))
    }
}
