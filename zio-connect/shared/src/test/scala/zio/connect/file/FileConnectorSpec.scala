package zio.connect.file

import zio.nio.file.{Files, Path}
import zio.{Cause, Chunk, Scope, ZIO}
import zio.stream.ZStream
import zio.test.{Spec, ZIOSpecDefault, assert, assertZIO}
import zio.test.Assertion.{equalTo, fails, failsCause, failsWithA}

import java.io.IOException
import java.util.UUID

trait FileConnectorSpec extends ZIOSpecDefault {

  val fileConnectorSpec: Spec[FileConnector with Scope, Throwable] =
    writeFileSuite + listDirSuite

  private lazy val writeFileSuite: Spec[FileConnector with Scope, Throwable] =
    suite("writeFile")(
      test("fails when IOException") {
        val ioException: IOException = new IOException("test ioException")
        val prog = {
          for {
            path         <- tempFile
            failingStream = ZStream(1).mapZIO(_ => ZIO.fail(ioException))
            sink         <- ZIO.serviceWith[FileConnector](_.writeFile(path))
            r            <- (failingStream >>> sink).exit
          } yield r
        }
        assertZIO(prog)(fails(equalTo(ioException)))
      },
      test("dies when failing without IOException") {
        object NonIOException extends Throwable
        val prog = for {
          path         <- tempFile
          failingStream = ZStream(1).mapZIO(_ => ZIO.fail(NonIOException))
          sink         <- ZIO.serviceWith[FileConnector](_.writeFile(path))
          r            <- (failingStream >>> sink).exit
        } yield r
        assertZIO(prog)(failsCause(equalTo(Cause.die(NonIOException))))
      },
      test("succeeds") {
        for {
          path   <- tempFile
          stream  = ZStream.fromChunk(Chunk[Byte](1, 2, 3))
          sink   <- ZIO.serviceWith[FileConnector](_.writeFile(path))
          _      <- stream >>> sink
          actual <- ZStream.fromPath(path.toFile.toPath).runCollect
        } yield assert(Chunk[Byte](1, 2, 3))(equalTo(actual))
      }
    )

  private lazy val listDirSuite: Spec[FileConnector with Scope, Throwable] =
    suite("listDir")(
      test("fails when IOException") {
        val prog = for {
          dir    <- tempDir
          stream <- ZIO.serviceWith[FileConnector](_.listDir(dir))
          _ <- Files.delete(dir) // delete the directory to cause an IOException
          r <- stream.runDrain.exit
        } yield r
        assertZIO(prog)(failsWithA[IOException])
      },
      test("succeeds") {
        for {
          dir              <- tempDir
          stream           <- ZIO.serviceWith[FileConnector](_.listDir(dir))
          file1            <- Files.createTempFileInScoped(dir, prefix = Some(UUID.randomUUID().toString))
          file2            <- Files.createTempFileInScoped(dir, prefix = Some(UUID.randomUUID().toString))
          file3            <- Files.createTempFileInScoped(dir, prefix = Some(UUID.randomUUID().toString))
          createdFilesPaths = Chunk(file1, file2, file3).map(_.toFile.getAbsolutePath).sorted
          r                <- stream.runCollect.map(files => files.map(_.toFile.getAbsolutePath).sorted)
        } yield assert(createdFilesPaths)(equalTo(r))
      }
    )

  lazy val tempFile: ZIO[Scope, Throwable, Path] =
    Files.createTempFileScoped(UUID.randomUUID().toString, None, List.empty)

  lazy val tempDir: ZIO[Scope, Throwable, Path] =
    Files.createTempDirectoryScoped(Some(UUID.randomUUID().toString), List.empty)

}
