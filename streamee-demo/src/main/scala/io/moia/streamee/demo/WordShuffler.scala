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

import io.moia.streamee.Process
import scala.annotation.tailrec
import scala.util.Random

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

  def shuffleWord(word: String): String = {
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
