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
  Step
}
import io.moia.streamee.either.{ EitherFlowWithContextOps, tapErrors }
import io.moia.streamee.demo.TextShuffler.Error.RandomError
import io.moia.streamee.demo.WordShuffler.{ ShuffleWord, WordShuffled }
import scala.collection.immutable.Seq
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration.FiniteDuration
import scala.util.Random

object TextShuffler {

  final case class ShuffleText(text: String)
  final case class TextShuffled(originalText: String, shuffledText: String)

  sealed trait Error
  object Error {
    final object EmptyText    extends Error
    final object InvalidText  extends Error
    final object EmptyWordSeq extends Error
    final object RandomError  extends Error
  }

  final case class Config(
      delay: FiniteDuration,
      wordShufflerProcessorTimeout: FiniteDuration,
      wordShufflerAskTimeout: FiniteDuration
  )

  private val validText = """[\w\s]*""".r // use a symbol like $ or * to fail this pattern

  def apply(config: Config, wordShufflerRunner: ActorRef[WordShufflerRunner.Command])(implicit
      mat: Materializer,
      ec: ExecutionContext,
      scheduler: Scheduler
  ): Process[ShuffleText, Either[Error, TextShuffled]] = {
    import config._

    def wordShufflerSinkRef(): Future[ProcessSinkRef[ShuffleWord, WordShuffled]] =
      wordShufflerRunner
        .ask(WordShufflerRunner.GetProcessSinkRef)(wordShufflerAskTimeout, scheduler)
        .recoverWith { case _ => wordShufflerSinkRef() }

    val wordShufflerSink =
      RestartSink.withBackoff(wordShufflerAskTimeout, wordShufflerAskTimeout, 0) { () =>
        Await.result(
          wordShufflerSinkRef().map(_.sink),
          wordShufflerAskTimeout
        ) // Hopefully we can get rid of blocking soon: https://github.com/akka/akka/issues/25934
      }

    tapErrors { errorTap =>
      Process[ShuffleText, Either[Error, TextShuffled]]
        .via(validateRequest)
        .errorTo(errorTap)
        .via(delayProcessing(delay))
        .via(randomError)
        .errorTo(errorTap)
        .via(keepSplitShuffle(wordShufflerSink, wordShufflerProcessorTimeout))
        .via(concat)
        .errorTo(errorTap) // not needed for finishing via(concat0)
    }
  }

  def validateRequest[Ctx]: Step[ShuffleText, Either[Error, ShuffleText], Ctx] =
    Step[ShuffleText, Ctx].map {
      case ShuffleText(text) if text.trim.isEmpty                        => Left(Error.EmptyText)
      case ShuffleText(text) if !validText.pattern.matcher(text).matches => Left(Error.InvalidText)
      case shuffleText                                                   => Right(shuffleText)
    }

  def delayProcessing[Ctx](of: FiniteDuration): Step[ShuffleText, ShuffleText, Ctx] =
    Step[ShuffleText, Ctx]
      .delay(of, DelayOverflowStrategy.backpressure)
      .withAttributes(Attributes.inputBuffer(1, 1))

  def randomError[Ctx]: Step[ShuffleText, Either[Error, ShuffleText], Ctx] =
    Step[ShuffleText, Ctx]
      .map(shuffleText => if (Random.nextBoolean()) Left(RandomError) else Right(shuffleText))

  def keepSplitShuffle[Ctx](
      wordShufflerSink: ProcessSink[WordShuffler.ShuffleWord, WordShuffler.WordShuffled],
      wordShufflerProcessorTimeout: FiniteDuration
  )(implicit mat: Materializer): Step[ShuffleText, (String, Seq[String]), Ctx] =
    Step[ShuffleText, Ctx]
      .map(_.text)
      .push // push the original text
      .map(_.split(" ").toList)
      .via(shuffleWords(wordShufflerSink, wordShufflerProcessorTimeout))
      .pop

  def shuffleWords[Ctx](
      wordShufflerSink: ProcessSink[WordShuffler.ShuffleWord, WordShuffler.WordShuffled],
      wordShufflerProcessorTimeout: FiniteDuration
  )(implicit mat: Materializer): Step[Seq[String], Seq[String], Ctx] =
    Step[Seq[String], Ctx]
      .mapAsync(1) { words =>
        Source(words)
          .map(WordShuffler.ShuffleWord)
          .into(wordShufflerSink, wordShufflerProcessorTimeout, 42)
          .runWith(Sink.seq)
      }
      .map(_.map(_.word))

  def concat[Ctx]: Step[(String, Seq[String]), Either[Error, TextShuffled], Ctx] =
    Step[(String, Seq[String]), Ctx].map {
      case (_, words) if words.isEmpty => Left(Error.EmptyWordSeq)
      case (text, words)               => Right(TextShuffled(text, words.mkString(" ")))
    }

  def concat0[Ctx]: Step[(String, Seq[String]), TextShuffled, Ctx] =
    Step[(String, Seq[String]), Ctx].map {
      case (text, words) => TextShuffled(text, words.mkString(" "))
    }
}
