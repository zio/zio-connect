package zio.connect

import zio.Trace
import zio.connect.fs2.FS2Connector.{FS2Exception, FS2Stream}
import zio.stream.ZStream

package object fs2 {

  def fromStream[A](
    original: FS2Stream[A],
    queueSize: Int = 16
  )(implicit
    trace: Trace
  ): ZStream[FS2Connector, FS2Exception, A] =
    ZStream.serviceWithStream[FS2Connector](_.fromStream(original, queueSize))

}
