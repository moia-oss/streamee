package io.moia

import akka.stream.scaladsl.SourceQueue
import scala.concurrent.Promise

package object streamee {

  /**
    * Convenient alias for the processor type.
    */
  type Processor[C, R] = SourceQueue[(C, Promise[R])]
}
