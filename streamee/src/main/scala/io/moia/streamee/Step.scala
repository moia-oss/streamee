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

import akka.stream.scaladsl.FlowWithContext

object Step {

  /** Create an empty initial [[Step]].
    *
    * @tparam In input type of the initial step
    * @tparam Ctx context type of the initial step
    * @return empty initial [[Step]]
    */
  def apply[In, Ctx]: Step[In, In, Ctx] =
    FlowWithContext[In, Ctx]
}
