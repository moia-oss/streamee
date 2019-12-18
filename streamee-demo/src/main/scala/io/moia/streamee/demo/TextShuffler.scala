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

import akka.actor.typed.{ ActorRef, Scheduler }
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.stream.{ Attributes, DelayOverflowStrategy, Materializer }
import akka.stream.scaladsl.{ RestartSink, Sink, Source }
import io.moia.streamee.{
  FlowWithContextExt,
  FlowWithPairedContextOps,
  Process,
  ProcessSink,
  ProcessSinkRef,
  SourceExt,
  Step,
  startProcess,
  startStep
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
  ): Process[ShuffleText, TextShuffled] = {
    import config._

    def wordShufflerSinkRef(): Future[ProcessSinkRef[ShuffleWord, WordShuffled]] =
      wordShufflerRunner
        .ask(WordShufflerRunner.GetProcessSinkRef)(wordShufflerAskTimeout, scheduler)
        .recoverWith { case _ => wordShufflerSinkRef() }

    val wordShufflerSink =
      RestartSink.withBackoff(wordShufflerAskTimeout, wordShufflerAskTimeout, 0) { () =>
        Await.result(wordShufflerSinkRef().map(_.sink), wordShufflerAskTimeout) // Hopefully we can get rid of blocking soon: https://github.com/akka/akka/issues/25934
      }

    startProcess[ShuffleText, TextShuffled]
      .via(delayRequest(delay))
      .via(keepSplitShuffle(wordShufflerSink, wordShufflerProcessorTimeout))
      .via(concat)
  }

  def delayRequest[Ctx](of: FiniteDuration): Step[ShuffleText, ShuffleText, Ctx] =
    startStep[ShuffleText, Ctx]
      .delay(of, DelayOverflowStrategy.backpressure)
      .withAttributes(Attributes.inputBuffer(1, 1))

  def keepSplitShuffle[Ctx](
      wordShufflerSink: ProcessSink[WordShuffler.ShuffleWord, WordShuffler.WordShuffled],
      wordShufflerProcessorTimeout: FiniteDuration
  )(implicit mat: Materializer): Step[ShuffleText, (String, Seq[String]), Ctx] =
    startStep[ShuffleText, Ctx]
      .map(_.text)
      .push // push the original text
      .map(_.split(" ").toList)
      .via(shuffleWords(wordShufflerSink, wordShufflerProcessorTimeout))
      .pop

  def shuffleWords[Ctx](
      wordShufflerSink: ProcessSink[WordShuffler.ShuffleWord, WordShuffler.WordShuffled],
      wordShufflerProcessorTimeout: FiniteDuration
  )(implicit mat: Materializer): Step[Seq[String], Seq[String], Ctx] =
    startStep[Seq[String], Ctx]
      .mapAsync(1) { words =>
        Source(words)
          .map(WordShuffler.ShuffleWord)
          .into(wordShufflerSink, wordShufflerProcessorTimeout, 42)
          .runWith(Sink.seq)
      }
      .map(_.map(_.word))

  def concat[Ctx]: Step[(String, Seq[String]), TextShuffled, Ctx] =
    startStep[(String, Seq[String]), Ctx].map {
      case (originalText, shuffledWords) => TextShuffled(originalText, shuffledWords.mkString(" "))
    }
}
