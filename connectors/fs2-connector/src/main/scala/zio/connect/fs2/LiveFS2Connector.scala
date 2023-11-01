package zio.connect.fs2

import zio._
import zio.connect.fs2.FS2Connector.{FS2Exception, FS2Stream}
import zio.interop.catz._
import zio.stream._

case class LiveFS2Connector() extends FS2Connector {

  override def fromStream[A](
    original: => FS2Stream[A],
    queueSize: => Int = 16
  )(implicit
    trace: Trace
  ): ZStream[Any, FS2Exception, A] = {
    def toZStreamSingle(implicit trace: Trace): ZStream[Any, FS2Exception, A] =
      ZStream.scoped {
        for {
          queue <- ZIO.acquireRelease(zio.Queue.bounded[Take[Throwable, A]](1))(_.shutdown)
          _ <- {
                 original
                   .evalTap(a => queue.offer(Take.single(a))) ++ fs2.Stream.eval(queue.offer(Take.end))
               }.handleErrorWith(e => fs2.Stream.eval(queue.offer(Take.fail(e))).drain)
                 .compile
                 .resource
                 .drain
                 .toScopedZIO
                 .forkScoped
        } yield ZStream.fromQueue(queue).flattenTake
      }.flatten
        .mapError(FS2Exception)

    def toZStreamChunk(queueSize: Int)(implicit trace: Trace): ZStream[Any, FS2Exception, A] =
      ZStream.scoped {
        for {
          queue <- ZIO.acquireRelease(zio.Queue.bounded[Take[Throwable, A]](queueSize))(_.shutdown)
          _ <- {
                 original
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
      }.flatten
        .mapError(FS2Exception)

    if (queueSize > 1) toZStreamChunk(queueSize) else toZStreamSingle
  }

}

object LiveFS2Connector {

  val layer: ZLayer[Any, Nothing, LiveFS2Connector] =
    ZLayer.succeed(LiveFS2Connector())

}
