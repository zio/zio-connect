package zio.connect.file

import zio.nio.file.{Files, Path}
import zio.{Cause, Chunk, Duration, Schedule, Scope, ZIO}
import zio.stream.{ZPipeline, ZStream}
import zio.test.{TestClock, ZIOSpecDefault, assert, assertZIO}
import zio.test.Assertion.{equalTo, fails, failsCause, failsWithA}
import zio.test.TestAspect.{flaky, withLiveClock}

import java.io.IOException
import java.nio.file.StandardOpenOption
import java.util.UUID

trait FileConnectorSpec extends ZIOSpecDefault {

  val fileConnectorSpec =
    writeFileSuite + listDirSuite + readFileSpec +
      tailFileSpec + tailFileUsingWatchServiceSpec + deleteFileSpec

  private lazy val writeFileSuite =
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

  private lazy val listDirSuite =
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

  private lazy val readFileSpec =
    suite("readFile")(
      test("fails when IOException") {
        val prog = for {
          file   <- tempFile
          stream <- ZIO.serviceWith[FileConnector](_.readFile(file))
          _ <- Files.delete(file) // delete the file to cause an IOException
          r <- stream.runDrain.exit
        } yield r
        assertZIO(prog)(failsWithA[IOException])
      },
      test("succeeds") {
        for {
          file   <- tempFile
          content = Chunk[Byte](1, 2, 3)
          _      <- Files.writeBytes(file, content)
          stream <- ZIO.serviceWith[FileConnector](_.readFile(file))
          r      <- stream.runCollect
        } yield assert(content)(equalTo(r))
      }
    )

  private lazy val tailFileSpec =
    suite("tailFile")(
      test("fails when IOException") {
        val prog = for {
          file   <- tempFile
          stream <- ZIO.serviceWith[FileConnector](_.tailFile(file, Duration.fromMillis(500)))
          _ <- Files.delete(file) // delete the file to cause an IOException
          fiber <- stream.runDrain.exit.fork
          _ <- TestClock
                 .adjust(Duration.fromMillis(3000))
          r <- fiber.join
        } yield r
        assertZIO(prog)(failsWithA[IOException])
      },
      test("succeeds") {
        val str = "test-value"
        val prog = for {
          file <- tempFile
          _ <- Files
                 .writeLines(file, List(str), openOptions = Set(StandardOpenOption.APPEND))
                 .repeat(Schedule.recurs(3) && Schedule.spaced(Duration.fromMillis(1000)))
                 .fork
          stream <- ZIO.serviceWith[FileConnector](_.tailFile(file, Duration.fromMillis(1000)))
          fiber <- stream
                     .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
                     .take(3)
                     .runCollect
                     .fork
          _ <- TestClock
                 .adjust(Duration.fromMillis(3000))
          r <- fiber.join
        } yield r
        assertZIO(prog)(equalTo(Chunk(str, str, str)))
      }
    )

  private lazy val tailFileUsingWatchServiceSpec =
    suite("tailFileUsingWatchService")(
      test("fails when IOException") {
        val prog = for {
          file   <- tempFile
          stream <- ZIO.serviceWith[FileConnector](_.tailFileUsingWatchService(file, Duration.fromMillis(500)))
          _ <- Files.delete(file) // delete the file to cause an IOException
          fiber <- stream.runDrain.exit.fork
          _ <- TestClock
                 .adjust(Duration.fromMillis(3000))
          r <- fiber.join
        } yield r
        assertZIO(prog)(failsWithA[IOException])
      },
      test("succeeds") {
        val str = "test-value"
        val prog = for {
          file   <- tempFile
          stream <- ZIO.serviceWith[FileConnector](_.tailFileUsingWatchService(file, Duration.fromMillis(500)))
          fiber <- stream
                     .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
                     .take(3)
                     .runCollect
                     .fork
          _ <- Files
                 .writeLines(file, List(str), openOptions = Set(StandardOpenOption.APPEND))
                 .repeat(Schedule.recurs(3) && Schedule.spaced(Duration.fromMillis(500)))
                 .fork
          r <- fiber.join.timeout(Duration.fromMillis(30000))
        } yield r
        assertZIO(prog)(equalTo(Some(Chunk(str, str, str))))
        //flaky as a backup to account for WatchService & fileSystem handling events eventually
      } @@ withLiveClock @@ flaky
    )

  private lazy val deleteFileSpec =
    suite("deleteFile")(
      test("fails when IOException") {
        val ioException: IOException = new IOException("test ioException")
        val prog = {
          for {
            path         <- tempFile
            failingStream = ZStream(1).mapZIO(_ => ZIO.fail(ioException))
            sink         <- ZIO.serviceWith[FileConnector](_.deleteFile(path))
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
          sink         <- ZIO.serviceWith[FileConnector](_.deleteFile(path))
          r            <- (failingStream >>> sink).exit
        } yield r
        assertZIO(prog)(failsCause(equalTo(Cause.die(NonIOException))))
      },
      test("succeeds") {
        for {
          file        <- tempFile
          sink        <- ZIO.serviceWith[FileConnector](_.deleteFile(file))
          _           <- ZStream.succeed(file) >>> sink
          fileDeleted <- Files.exists(file).map(!_)
        } yield assert(fileDeleted)(equalTo(true))
      }
    )

  lazy val tempFile: ZIO[Scope, Throwable, Path] =
    Files.createTempFileScoped(UUID.randomUUID().toString, None, List.empty)

  lazy val tempDir: ZIO[Scope, Throwable, Path] =
    Files.createTempDirectoryScoped(Some(UUID.randomUUID().toString), List.empty)

}
