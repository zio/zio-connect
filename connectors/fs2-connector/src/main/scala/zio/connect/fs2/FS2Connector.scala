package zio.connect.fs2

import cats.effect.kernel.Async
import zio._
import zio.stream.ZStream

trait FS2Connector {

  def fromStream[F[_], A](original: fs2.Stream[F, A])(implicit trace: Trace): ZStream[Any, Throwable, A]

  def toStream[F[_]: Async, R: Runtime, A](original: ZStream[R, Throwable, A])(implicit trace: Trace): fs2.Stream[F, A]

}
