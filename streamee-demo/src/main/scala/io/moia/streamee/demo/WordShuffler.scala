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

import akka.actor.typed.{ ActorRef, Behavior }
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.Materializer
import io.moia.streamee.{ IntoableProcessor, Process, ProcessSinkRef }
import org.apache.logging.log4j.scala.Logging
import scala.annotation.tailrec
import scala.util.{ Failure, Random, Success }

object WordShuffler {

  final case class ShuffleWord(word: String)
  final case class WordShuffled(word: String)

  def apply(): Process[ShuffleWord, WordShuffled, WordShuffled] =
    Process[ShuffleWord, WordShuffled]()
      .via(shuffleWordToString)
      .via(shuffle)
      .via(stringToWordShuffled)

  def shuffleWordToString: Process[ShuffleWord, String, WordShuffled] =
    Process[ShuffleWord, WordShuffled]().map(_.word)

  def shuffle: Process[String, String, WordShuffled] =
    Process[String, WordShuffled]().map(shuffleWord)

  def stringToWordShuffled: Process[String, WordShuffled, WordShuffled] =
    Process().map(WordShuffled)

  private def shuffleWord(word: String) = {
    @tailrec def loop(word: String, acc: String = ""): String =
      if (word.isEmpty)
        acc
      else {
        val (left, right) = word.splitAt(Random.nextInt(word.length))
        val c             = right.head
        val nextWord      = left + right.tail
        loop(nextWord, c + acc)
      }

    if (word.length <= 3)
      word
    else {
      word.head + loop(word.tail.init) + word.last
    }
  }
}

object WordShufflerRunner extends Logging {
  import WordShuffler._

  sealed trait Command
  final case class GetProcessSinkRef(replyTo: ActorRef[ProcessSinkRef[ShuffleWord, WordShuffled]])
      extends Command
  final case object Shutdown     extends Command
  private final case object Stop extends Command

  def apply()(implicit mat: Materializer): Behavior[Command] =
    Behaviors.setup { context =>
      import context.executionContext

      val self                  = context.self
      val wordShufflerProcessor = IntoableProcessor(WordShuffler(), "word-shuffler")

      wordShufflerProcessor.whenDone.onComplete { reason =>
        logger.warn(s"Process completed: $reason")
        self ! Stop
      }

      Behaviors.receiveMessagePartial {
        case GetProcessSinkRef(replyTo) =>
          wordShufflerProcessor
            .sinkRef()
            .onComplete {
              case Success(sinkRef) => replyTo ! sinkRef
              case Failure(cause)   => logger.error("Cannot create ProcessSinkRef!", cause)
            }
          Behaviors.same

        case Shutdown =>
          wordShufflerProcessor.shutdown()
          Behaviors.receiveMessagePartial { case Stop => Behaviors.stopped }
      }
    }
}
