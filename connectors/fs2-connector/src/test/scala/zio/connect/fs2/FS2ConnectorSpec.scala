package zio.connect.fs2

import fs2.Stream
import zio.Random.nextIntBetween
import zio.connect.fs2.FS2Connector.{FS2Exception, FS2Stream}
import zio.interop.catz._
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._
import zio.{Chunk, Ref, Task, ZIO}

trait FS2ConnectorSpec extends ZIOSpecDefault {

  val fs2ConnectorSpec = fromStreamSpec

  private lazy val fromStreamSpec =
    suite("fromStream")(
      test("simple stream")(check(Gen.chunkOf(Gen.int)) { (chunk: Chunk[Int]) =>
        assertEqual(
          fromStream(fs2StreamFromChunk(chunk)),
          ZStream.fromChunk(chunk)
        )
      }),
      test("non empty stream")(check(Gen.chunkOf1(Gen.long)) { chunk =>
        assertEqual(fromStream(fs2StreamFromChunk(chunk)), ZStream.fromChunk(chunk))
      }),
      test("100 element stream")(check(Gen.chunkOfN(100)(Gen.long)) { chunk =>
        assertEqual(fromStream(fs2StreamFromChunk(chunk)), ZStream.fromChunk(chunk))
      }),
      test("error propagation") {
        val result = fromStream(Stream.raiseError[Task](exception)).runDrain.exit
        assertZIO(result)(fails(equalTo(FS2Exception(exception))))
      },
      test("releases all resources by the time the failover stream has started") {
        for {
          queueSize <- nextIntBetween(2, 32)
          fins      <- Ref.make(Chunk[Int]())
          stream = Stream(1).onFinalize(fins.update(1 +: _)) >>
                     Stream(2).onFinalize(fins.update(2 +: _)) >>
                     Stream(3).onFinalize(fins.update(3 +: _)) >>
                     Stream.raiseError[Task](exception)
          result <- fromStream(stream, queueSize).drain
                      .catchAllCause(_ => ZStream.fromZIO(fins.get))
                      .runCollect
        } yield assert(result.flatten)(equalTo(Chunk(1, 2, 3)))
      },
      test("bigger queueSize than a chunk size")(check(Gen.chunkOfN(10)(Gen.long)) { chunk =>
        for {
          queueSize <- nextIntBetween(32, 256)
          result <- assertEqual(
                      fromStream(fs2StreamFromChunk(chunk), queueSize),
                      ZStream.fromChunk(chunk)
                    )
        } yield result
      }),
      test("queueSize == 1")(check(Gen.chunkOfN(10)(Gen.long)) { chunk =>
        assertEqual(
          fromStream(fs2StreamFromChunk(chunk), 1),
          ZStream.fromChunk(chunk)
        )
      }),
      test("negative queueSize")(check(Gen.chunkOfN(10)(Gen.long)) { chunk =>
        for {
          queueSize <- nextIntBetween(-128, 0)
          result <- assertEqual(
                      fromStream(fs2StreamFromChunk(chunk), queueSize),
                      ZStream.fromChunk(chunk)
                    )
        } yield result
      }),
      test("RIO")(check(Gen.chunkOfN(10)(Gen.long)) { chunk =>
        for {
          queueSize <- nextIntBetween(2, 128)
          result <- assertEqual(
                      fromStream(fs2StreamFromChunk(chunk).covary[Task], queueSize),
                      ZStream.fromChunk(chunk)
                    )
        } yield result
      })
    )

  private val exception: Throwable = new Exception("Failed")

  private def fs2StreamFromChunk[A](chunk: Chunk[A]): FS2Stream[A] =
    fs2.Stream.chunk[Task, A](fs2.Chunk.indexedSeq(chunk))

  private def assertEqual[R, E, A](actual: ZStream[R, E, A], expected: ZStream[R, E, A]): ZIO[R, E, TestResult] =
    for {
      x <- actual.runCollect
      y <- expected.runCollect
    } yield assert(x)(equalTo(y))

}
