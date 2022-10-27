package zio.connect

import _root_.fs2.Stream
import cats.effect.kernel.Async
import cats.effect.std.Dispatcher
import zio.stream.ZStream
import zio.{RIO, Runtime, Trace, ZIO}

package object fs2 {

  def fromStream[F[_]: Dispatcher, R, A](
    original: Stream[F, A],
    queueSize: Int = 16
  )(implicit
    trace: Trace
  ): ZStream[FS2Connector with R, Throwable, A] =
    ZStream.serviceWithStream[FS2Connector](_.fromStream(original, queueSize))

  def toStream[F[_]: Async, R: Runtime, A](
    original: ZStream[R, Throwable, A]
  )(implicit trace: Trace): RIO[FS2Connector with R, Stream[F, A]] =
    ZIO.serviceWith[FS2Connector](_.toStream(original))

}
