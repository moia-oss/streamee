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

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import scala.concurrent.duration.DurationInt

final class RespondeeTests extends AsyncWordSpec with AkkaSuite with Matchers {
  import Respondee._

  "A Respondee" should {
    "fail its promise with a TimeoutException if not receiving a Response in time" in {
      val timeout       = 100.milliseconds
      val (_, response) = Respondee.spawn[Int](timeout)
      response.future.failed.map { case ResponseTimeoutException(t) => t shouldBe timeout }
    }

    "successfully complete its promise with the received Response" in {
      val (respondee, response) = Respondee.spawn[Int](1.second)
      respondee ! Response(42)
      response.future.map(_ shouldBe 42)
    }
  }
}
