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

import org.scalatest.{ AsyncWordSpec, Matchers }
import scala.concurrent.duration.DurationInt

final class PushPopTests extends AsyncWordSpec with ActorTestSuite with Matchers {

  "Calling pushIn and popIn" should {
    "propagate the input element to the output" in {
      import untypedSystem.dispatcher
      val process   = Process[String, (String, Int)]().map(_.toUpperCase).push.map(_.length).pop
      val processor = FrontProcessor(process, 1.second, "processor")
      processor
        .accept("abc")
        .map {
          case (s, n) =>
            s shouldBe "ABC"
            n shouldBe 4
        }
    }
  }
}
