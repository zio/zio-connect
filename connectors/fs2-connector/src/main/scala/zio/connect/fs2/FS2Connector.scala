package zio.connect.fs2

import cats.effect.kernel.Async
import cats.effect.std.Dispatcher
import zio._
import zio.stream.ZStream

trait FS2Connector {

  def fromStream[F[_]: Dispatcher, R, A](original: fs2.Stream[F, A], queueSize: Int = 16)(implicit
    trace: Trace
  ): ZStream[R, Throwable, A]

  def toStream[F[_]: Async, R: Runtime, A](original: ZStream[R, Throwable, A])(implicit trace: Trace): fs2.Stream[F, A]

}
