package zio.connect.file

import zio.nio.file.{ Files, Path }
import zio.{ Cause, Chunk, Scope, ZIO }
import zio.stream.ZStream
import zio.test.assert
import zio.test.Assertion.{ equalTo, failsCause, failsWithA }
import zio.test.{ assertZIO, ZIOSpecDefault }

import java.io.IOException
import java.util.UUID

object LiveFileConnectorSpec extends ZIOSpecDefault {

  val target = LiveFileConnector()

  override def spec =
    suite("LiveFileConnectorSpec")(
      suite("writeFile")(
        test("fails when IOException") {
          val ioException: IOException = new IOException("test ioException")
          val prog = tempFile.flatMap { path =>
            val s = target.writeFile(path)
            ZStream(1).mapZIO(_ => ZIO.fail(ioException)).run(s).exit
          }
          assertZIO(prog)(failsWithA[IOException])
        },
        test("dies when not IOException") {
          object NonIOException extends Throwable
          val prog = tempFile.flatMap { path =>
            val s = target.writeFile(path)
            ZStream(1).mapZIO(_ => ZIO.fail(NonIOException)).run(s).exit
          }
          assertZIO(prog)(failsCause(equalTo(Cause.die(NonIOException))))
        },
        test("succeeds") {
          for {
            path   <- tempFile
            s      = target.writeFile(path)
            _      <- ZStream.fromChunk(Chunk[Byte](1, 2, 3)).run(s)
            actual <- ZStream.fromPath(path.toFile.toPath).runCollect
          } yield assert(actual)(equalTo(Chunk[Byte](1, 2, 3)))
        }
      )
    )

  val tempFile: ZIO[Scope, Throwable, Path] =
    ZIO.acquireRelease(Files.createTempFile(UUID.randomUUID().toString, None, List.empty))(f => Files.delete(f).orDie)
}
