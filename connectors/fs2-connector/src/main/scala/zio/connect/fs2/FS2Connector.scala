package zio.connect.fs2

import zio._
import zio.connect.fs2.FS2Connector._
import zio.stream.ZStream

trait FS2Connector {

  def fromStream[A](
    original: => FS2Stream[A],
    queueSize: => Int = 16
  )(implicit
    trace: Trace
  ): ZStream[Any, FS2Exception, A]

}

object FS2Connector {

  type FS2Stream[A] = fs2.Stream[Task, A]

  case class FS2Exception(reason: Throwable)

}
