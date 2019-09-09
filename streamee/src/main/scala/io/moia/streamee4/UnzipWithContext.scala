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

package io.moia.streamee4

import akka.stream.scaladsl.{ Broadcast, GraphDSL, Unzip, Zip }
import akka.stream.{ FanOutShape2, Graph }

/**
  * Like standard `Unzip` but propagating a context object.
  */
object UnzipWithContext {
  // Why not [Ctx, In1, In2]? See https://github.com/akka/akka/issues/26345
  def apply[In1, In2, Ctx](): Graph[FanOutShape2[((In1, In2), Ctx), (In1, Ctx), (In2, Ctx)], Any] =
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val unzipInsCtx = builder.add(Unzip[(In1, In2), Ctx]())
      val unzipIn1In2 = builder.add(Unzip[In1, In2]())
      val bcastCtx    = builder.add(Broadcast[Ctx](2, eagerCancel = true))
      val zipIn1Ctx   = builder.add(Zip[In1, Ctx]())
      val zipIn2Ctx   = builder.add(Zip[In2, Ctx]())

      // format: off
                          unzipIn1In2.out0 ~> zipIn1Ctx.in0
                          unzipIn1In2.out1 ~> zipIn2Ctx.in0
      unzipInsCtx.out0 ~> unzipIn1In2.in
      unzipInsCtx.out1 ~> bcastCtx.in
                          bcastCtx.out(0)  ~> zipIn2Ctx.in1
                          bcastCtx.out(1)  ~> zipIn1Ctx.in1
      // format: on

      new FanOutShape2(unzipInsCtx.in, zipIn1Ctx.out, zipIn2Ctx.out)
    }
}
