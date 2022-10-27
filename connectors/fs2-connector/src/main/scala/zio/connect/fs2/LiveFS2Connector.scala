package zio.connect.fs2

import cats.effect.Resource
import cats.effect.kernel.Async
import cats.~>
import zio._
import zio.stream.ZStream
import zio.interop._
import zio.interop.catz._

case class LiveFS2Connector() extends FS2Connector {

  override def fromStream[F[_], A](original: fs2.Stream[F, A])(implicit trace: Trace): ZStream[Any, Throwable, A] = ???

  override def toStream[F[_]: Async, R: Runtime, A](
    original: ZStream[R, Throwable, A]
  )(implicit trace: Trace): fs2.Stream[F, A] =
    fs2.Stream
      .resource(Resource.scopedZIO[R, Throwable, ZIO[R, Option[Throwable], Chunk[A]]](original.toPull))
      .flatMap { pull =>
        fs2.Stream
          .repeatEval(pull.unsome)
          .unNoneTerminate
          .flatMap { chunk =>
            fs2.Stream.chunk(fs2.Chunk.indexedSeq(chunk))
          }
      }
      .translate(fk[F, R])

  private def fk[F[_]: Async, R: Runtime]: RIO[R, *] ~> F = new (RIO[R, *] ~> F) {
    override def apply[A](fa: RIO[R, A]): F[A] = toEffect[F, R, A](fa)
  }

}

object LiveFS2Connector {

  val layer: ZLayer[Any, Nothing, LiveFS2Connector] =
    ZLayer.succeed(LiveFS2Connector())

}
