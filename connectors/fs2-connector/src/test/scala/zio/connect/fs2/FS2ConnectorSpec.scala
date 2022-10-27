package zio.connect.fs2

import cats.effect.Concurrent
import cats.effect.kernel.Async
import cats.effect.std.Dispatcher
import cats.syntax.flatMap._
import cats.syntax.functor._
import fs2.Stream
import zio.Random.nextIntBetween
import zio.interop.catz._
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._
import zio.{Chunk, RIO, Ref, Runtime, ZIO}

trait FS2ConnectorSpec extends ZIOSpecDefault {

  val fs2ConnectorSpec = toStreamSpec + fromStreamSpec

  implicit private val runtime0: Runtime[Any] = runtime

  private lazy val toStreamSpec =
    suite("toStream")(
      test("simple stream")(check(Gen.chunkOf(Gen.int)) { (chunk: Chunk[Int]) =>
        ZIO.serviceWithZIO[FS2Connector] { service =>
          assertEqual(
            service.toStream[RIO[Any, *], Any, Int](ZStream.fromChunk(chunk)),
            fs2StreamFromChunk[RIO[Any, *], Int](chunk)
          )
        }
      }),
      test("non empty stream")(check(Gen.chunkOf1(Gen.long)) { chunk =>
        ZIO.serviceWithZIO[FS2Connector] { service =>
          assertEqual(
            service.toStream[RIO[Any, *], Any, Long](ZStream.fromChunk(chunk)),
            fs2StreamFromChunk[RIO[Any, *], Long](chunk)
          )
        }
      }),
      test("100 element stream")(check(Gen.chunkOfN(100)(Gen.long)) { chunk =>
        ZIO.serviceWithZIO[FS2Connector] { service =>
          assertEqual(service.toStream[RIO[Any, *], Any, Long](ZStream.fromChunk(chunk)), fs2StreamFromChunk(chunk))
        }
      }),
      test("error propagation") {
        ZIO.serviceWithZIO[FS2Connector] { service =>
          val result = service.toStream[RIO[Any, *], Any, Nothing](ZStream.fail(exception)).compile.drain.exit
          assertZIO(result)(fails(equalTo(exception)))
        }
      }
    )

  private lazy val fromStreamSpec =
    suite("fromStream")(
      test("simple stream")(check(Gen.chunkOf(Gen.int)) { (chunk: Chunk[Int]) =>
        withDispatcher[RIO[FS2Connector, *]] { implicit dispatcher =>
          assertEqual(fromStream(fs2StreamFromChunk[RIO[FS2Connector, *], Int](chunk)), ZStream.fromChunk(chunk))
        }
      }),
      test("non empty stream")(check(Gen.chunkOf1(Gen.long)) { chunk =>
        withDispatcher[RIO[FS2Connector, *]] { implicit dispatcher =>
          assertEqual(fromStream(fs2StreamFromChunk(chunk)), ZStream.fromChunk(chunk))
        }
      }),
      test("100 element stream")(check(Gen.chunkOfN(100)(Gen.long)) { chunk =>
        withDispatcher[RIO[FS2Connector, *]] { implicit dispatcher =>
          assertEqual(fromStream(fs2StreamFromChunk(chunk)), ZStream.fromChunk(chunk))
        }
      }),
      test("error propagation") {
        withDispatcher[RIO[FS2Connector, *]] { implicit dispatcher =>
          val result = fromStream(Stream.raiseError[RIO[FS2Connector, *]](exception)).runDrain.exit
          assertZIO(result)(fails(equalTo(exception)))
        }
      },
      test("releases all resources by the time the failover stream has started") {
        withDispatcher[RIO[FS2Connector, *]] { implicit dispatcher =>
          for {
            queueSize <- nextIntBetween(2, 32)
            fins      <- Ref.make(Chunk[Int]())
            stream = Stream(1).onFinalize(fins.update(1 +: _)) >>
                       Stream(2).onFinalize(fins.update(2 +: _)) >>
                       Stream(3).onFinalize(fins.update(3 +: _)) >>
                       Stream.raiseError[RIO[FS2Connector, *]](exception)
            result <- fromStream(stream, queueSize).drain.catchAllCause(_ => ZStream.fromZIO(fins.get)).runCollect
          } yield assert(result.flatten)(equalTo(Chunk(1, 2, 3)))
        }
      },
      test("bigger queueSize than a chunk size")(check(Gen.chunkOfN(10)(Gen.long)) { chunk =>
        withDispatcher[RIO[FS2Connector, *]] { implicit dispatcher =>
          for {
            queueSize <- nextIntBetween(32, 256)
            result    <- assertEqual(fromStream(fs2StreamFromChunk(chunk), queueSize), ZStream.fromChunk(chunk))
          } yield result
        }
      }),
      test("queueSize == 1")(check(Gen.chunkOfN(10)(Gen.long)) { chunk =>
        withDispatcher[RIO[FS2Connector, *]] { implicit dispatcher =>
          assertEqual(fromStream(fs2StreamFromChunk(chunk), 1), ZStream.fromChunk(chunk))
        }
      }),
      test("negative queueSize")(check(Gen.chunkOfN(10)(Gen.long)) { chunk =>
        withDispatcher[RIO[FS2Connector, *]] { implicit dispatcher =>
          for {
            queueSize <- nextIntBetween(-128, 0)
            result    <- assertEqual(fromStream(fs2StreamFromChunk(chunk), queueSize), ZStream.fromChunk(chunk))
          } yield result
        }
      }),
      test("RIO")(check(Gen.chunkOfN(10)(Gen.long)) { chunk =>
        withDispatcher[RIO[FS2Connector, *]] { implicit dispatcher =>
          for {
            queueSize <- nextIntBetween(2, 128)
            result <- assertEqual(
                        fromStream(fs2StreamFromChunk(chunk).covary[RIO[FS2Connector, *]], queueSize),
                        ZStream.fromChunk(chunk)
                      )
          } yield result
        }
      })
    )

  private val exception: Throwable = new Exception("Failed")

  private def fs2StreamFromChunk[F[_], A](chunk: Chunk[A]): fs2.Stream[F, A] =
    fs2.Stream.chunk[F, A](fs2.Chunk.indexedSeq(chunk))

  private def assertEqual[F[_]: Concurrent, A](actual: fs2.Stream[F, A], expected: fs2.Stream[F, A]): F[TestResult] =
    for {
      x <- actual.compile.toVector
      y <- expected.compile.toVector
    } yield assert(x)(equalTo(y))

  private def assertEqual[R, E, A](actual: ZStream[R, E, A], expected: ZStream[R, E, A]): ZIO[R, E, TestResult] =
    for {
      x <- actual.runCollect
      y <- expected.runCollect
    } yield assert(x)(equalTo(y))

  private def withDispatcher[F[_]: Async](body: Dispatcher[F] => F[TestResult]): F[TestResult] =
    Dispatcher[F].use(body)

}
