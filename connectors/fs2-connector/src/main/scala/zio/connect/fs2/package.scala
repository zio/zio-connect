package zio.connect

import zio.Trace
import zio.stream.ZStream
import _root_.fs2.Stream
import cats.effect.std.Dispatcher

package object fs2 {

  def fromStream[F[_]: Dispatcher, R, A](
    original: Stream[F, A],
    queueSize: Int = 16
  )(implicit
    trace: Trace
  ): ZStream[FS2Connector with R, Throwable, A] =
    ZStream.serviceWithStream[FS2Connector](_.fromStream(original, queueSize))

}
