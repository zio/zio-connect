package zio.connect.file

import zio.nio.file.{Files, Path}
import zio.{Cause, Chunk, Scope, ZIO}
import zio.stream.ZStream
import zio.test.{assert, assertZIO, Spec, ZIOSpecDefault}
import zio.test.Assertion.{equalTo, fails, failsCause}

import java.io.IOException
import java.util.UUID

trait FileConnectorSpec extends ZIOSpecDefault {

  val fileConnectorSpec: Spec[FileConnector with Scope, Throwable] =
    suite("writeFile")(
      test("fails when IOException") {
        val ioException: IOException = new IOException("test ioException")
        val prog = {
          for {
            path <- tempFile
            sink <- ZIO.serviceWith[FileConnector](_.writeFile(path))
            r    <- ZStream(1).mapZIO(_ => ZIO.fail(ioException)).run(sink).exit
          } yield r
        }
        assertZIO(prog)(fails(equalTo(ioException)))
      },
      test("dies when not IOException") {
        object NonIOException extends Throwable
        val prog = for {
          path <- tempFile
          sink <- ZIO.serviceWith[FileConnector](_.writeFile(path))
          r    <- ZStream(1).mapZIO(_ => ZIO.fail(NonIOException)).run(sink).exit
        } yield r
        assertZIO(prog)(failsCause(equalTo(Cause.die(NonIOException))))
      },
      test("succeeds") {
        for {
          path   <- tempFile
          sink   <- ZIO.serviceWith[FileConnector](_.writeFile(path))
          _      <- ZStream.fromChunk(Chunk[Byte](1, 2, 3)).run(sink)
          actual <- ZStream.fromPath(path.toFile.toPath).runCollect
        } yield assert(Chunk[Byte](1, 2, 3))(equalTo(actual))
      }
    )

  lazy val tempFile: ZIO[Scope, Throwable, Path] =
    ZIO.acquireRelease(Files.createTempFile(UUID.randomUUID().toString, None, List.empty))(f => Files.delete(f).orDie)

}
