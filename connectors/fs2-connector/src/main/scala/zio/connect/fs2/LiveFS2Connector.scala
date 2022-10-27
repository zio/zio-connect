package zio.connect.fs2

import cats.effect.Resource
import cats.effect.kernel.Async
import cats.effect.std.Dispatcher
import cats.~>
import zio._
import zio.stream._
import zio.interop._
import zio.interop.catz._

case class LiveFS2Connector() extends FS2Connector {

  override def fromStream[F[_]: Dispatcher, R, A](
    original: fs2.Stream[F, A],
    queueSize: Int = 16
  )(implicit
    trace: Trace
  ): ZStream[R, Throwable, A] = {
    def toZStreamSingle[R1 <: R](implicit trace: Trace): ZStream[R1, Throwable, A] =
      ZStream
        .scoped[R1] {
          for {
            queue <- ZIO.acquireRelease(zio.Queue.bounded[Take[Throwable, A]](1))(_.shutdown)
            _ <- {
              original
                .translate(fkFZio)
                .evalTap(a => queue.offer(Take.single(a))) ++ fs2.Stream.eval(queue.offer(Take.end))
            }.handleErrorWith(e => fs2.Stream.eval(queue.offer(Take.fail(e))).drain)
              .compile
              .resource
              .drain
              .toScopedZIO
              .forkScoped
          } yield ZStream.fromQueue(queue).flattenTake
        }
        .flatten

    def toZStreamChunk[R1 <: R](queueSize: Int)(implicit trace: Trace): ZStream[R1, Throwable, A] =
      ZStream
        .scoped[R1] {
          for {
            queue <- ZIO.acquireRelease(zio.Queue.bounded[Take[Throwable, A]](queueSize))(_.shutdown)
            _ <- {
              original
                .translate(fkFZio)
                .chunkLimit(queueSize)
                .evalTap(a => queue.offer(Take.chunk(zio.Chunk.fromIterable(a.toList))))
                .chunkLimit(1)
                .unchunks ++ fs2.Stream.eval(queue.offer(Take.end))
            }.handleErrorWith(e => fs2.Stream.eval(queue.offer(Take.fail(e))).drain)
              .compile
              .resource
              .drain
              .toScopedZIO
              .forkScoped
          } yield ZStream.fromQueue(queue).flattenTake
        }
        .flatten

    if (queueSize > 1) toZStreamChunk(queueSize) else toZStreamSingle
  }

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
      .translate(fkZioF[F, R])

  private def fkZioF[F[_]: Async, R: Runtime](implicit trace: Trace): RIO[R, *] ~> F = new (RIO[R, *] ~> F) {
    override def apply[A](fa: RIO[R, A]): F[A] = fa.toEffect[F]
  }

  private def fkFZio[F[_]: Dispatcher](implicit trace: Trace): F ~> Task = new (F ~> Task) {
    override def apply[A](fa: F[A]): Task[A] = fromEffect(fa)
  }

}

object LiveFS2Connector {

  val layer: ZLayer[Any, Nothing, LiveFS2Connector] =
    ZLayer.succeed(LiveFS2Connector())

}
