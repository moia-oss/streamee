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

package io.moia

import akka.stream.scaladsl.FlowWithContext
import scala.concurrent.Promise

package object streamee4 {

  /**
    * A domain logic process from request to response which transparently propagates a promise for
    * the response .
    *
    * @tparam Req request type
    * @tparam Res response type
    */
  type Process[-Req, Res] = ProcessStage[Req, Res, Res]

  /**
    * A part of a [[Process]].
    *
    * @tparam In input type
    * @tparam Out output type
    * @tparam Res overall process response type
    */
  type ProcessStage[-In, Out, Res] = FlowWithContext[Promise[Res], In, Promise[Res], Out, Any]
}
