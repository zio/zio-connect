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
    writeFileSuite + listDirSuite + readFileSuite +
      tailFileSuite + tailFileUsingWatchServiceSuite +
      deleteFileSuite + moveFileSuite

  private lazy val writeFileSuite =
    suite("writeFile")(
      test("fails when IOException") {
        val ioException: IOException = new IOException("test ioException")
        val prog = {
          for {
            path         <- tempFile
            failingStream = ZStream(1).mapZIO(_ => ZIO.fail(ioException))
            sink          = FileConnector.writeFile(path)
            r            <- (failingStream >>> sink).exit
          } yield r
        }
        assertZIO(prog)(fails(equalTo(ioException)))
      },
      test("dies when non-IOException exception") {
        object NonIOException extends Throwable
        val prog = for {
          path         <- tempFile
          failingStream = ZStream(1).mapZIO(_ => ZIO.fail(NonIOException))
          sink          = FileConnector.writeFile(path)
          r            <- (failingStream >>> sink).exit
        } yield r
        assertZIO(prog)(failsCause(equalTo(Cause.die(NonIOException))))
      },
      test("overwrites existing file") {
        for {
          path            <- tempFile
          existingContents = Chunk[Byte](4, 5, 6, 7)
          _               <- Files.writeBytes(path, existingContents)
          input            = Chunk[Byte](1, 2, 3)
          _               <- ZStream.fromChunk(input) >>> FileConnector.writeFile(path)
          actual          <- ZStream.fromPath(path.toFile.toPath).runCollect
        } yield assert(input)(equalTo(actual))
      },
      test("creates and writes to file") {
        for {
          path   <- tempFile
          _      <- Files.delete(path)
          input   = Chunk[Byte](1, 2, 3)
          _      <- ZStream.fromChunk(input) >>> FileConnector.writeFile(path)
          actual <- ZStream.fromPath(path.toFile.toPath).runCollect
        } yield assert(input)(equalTo(actual))
      }
    )

  private lazy val listDirSuite =
    suite("listDir")(
      test("fails when IOException") {
        val prog = for {
          dir   <- tempDir
          stream = FileConnector.listDir(dir)
          _ <- Files.delete(dir) // delete the directory to cause an IOException
          r <- stream.runDrain.exit
        } yield r
        assertZIO(prog)(failsWithA[IOException])
      },
      test("succeeds") {
        for {
          dir              <- tempDir
          stream            = FileConnector.listDir(dir)
          file1            <- Files.createTempFileInScoped(dir, prefix = Some(UUID.randomUUID().toString))
          file2            <- Files.createTempFileInScoped(dir, prefix = Some(UUID.randomUUID().toString))
          file3            <- Files.createTempFileInScoped(dir, prefix = Some(UUID.randomUUID().toString))
          createdFilesPaths = Chunk(file1, file2, file3).map(_.toFile.getAbsolutePath).sorted
          r                <- stream.runCollect.map(files => files.map(_.toFile.getAbsolutePath).sorted)
        } yield assert(createdFilesPaths)(equalTo(r))
      }
    )

  private lazy val readFileSuite =
    suite("readFile")(
      test("fails when IOException") {
        val prog = for {
          file  <- tempFile
          stream = FileConnector.readFile(file)
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
          stream  = FileConnector.readFile(file)
          r      <- stream.runCollect
        } yield assert(content)(equalTo(r))
      }
    )

  private lazy val tailFileSuite =
    suite("tailFile")(
      test("fails when IOException") {
        val prog = for {
          file  <- tempFile
          stream = FileConnector.tailFile(file, Duration.fromMillis(500))
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
          stream = FileConnector.tailFile(file, Duration.fromMillis(1000))
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

  private lazy val tailFileUsingWatchServiceSuite =
    suite("tailFileUsingWatchService")(
      test("fails when IOException") {
        val prog = for {
          file  <- tempFile
          stream = FileConnector.tailFileUsingWatchService(file, Duration.fromMillis(500))
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
          file  <- tempFile
          stream = FileConnector.tailFileUsingWatchService(file, Duration.fromMillis(500))
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

  private lazy val deleteFileSuite =
    suite("deleteFile")(
      test("fails when IOException") {
        val prog = {
          for {
            path <- tempFile
            _    <- Files.delete(path)
            r    <- (ZStream(path) >>> FileConnector.deleteFile).exit
          } yield r
        }
        assertZIO(prog)(failsWithA[IOException])
      },
      test("dies when non-IOException exception") {
        object NonIOException extends Throwable
        val prog = for {
          path         <- tempFile
          failingStream = ZStream(path).mapZIO(_ => ZIO.fail(NonIOException))
          r            <- (failingStream >>> FileConnector.deleteFile).exit
        } yield r
        assertZIO(prog)(failsCause(equalTo(Cause.die(NonIOException))))
      },
      test("succeeds") {
        for {
          file        <- tempFile
          _           <- ZStream.succeed(file) >>> FileConnector.deleteFile
          fileDeleted <- Files.exists(file).map(!_)
        } yield assert(fileDeleted)(equalTo(true))
      }
    )

  private lazy val moveFileSuite =
    suite("moveFile")(
      test("fails when IOException") {
        val ioException: IOException = new IOException("test ioException")
        val prog = {
          for {
            path           <- tempFile
            newDir         <- tempDir
            destinationPath = Path(newDir.toString(), path.toFile.getName)
            failingStream   = ZStream(path).mapZIO(_ => ZIO.fail(ioException))
            sink            = FileConnector.moveFile(_ => destinationPath)
            r              <- (failingStream >>> sink).exit
          } yield r
        }
        assertZIO(prog)(fails(equalTo(ioException)))
      },
      test("dies when non-IOException exception") {
        object NonIOException extends Throwable
        val prog = {
          for {
            path           <- tempFile
            newDir         <- tempDir
            destinationPath = Path(newDir.toString(), path.toFile.getName)
            failingStream   = ZStream(path).mapZIO(_ => ZIO.fail(NonIOException))
            sink            = FileConnector.moveFile(_ => destinationPath)
            r              <- (failingStream >>> sink).exit
          } yield r
        }
        assertZIO(prog)(failsCause(equalTo(Cause.die(NonIOException))))
      },
      test("succeeds") {
        for {
          sourcePath     <- tempFile
          lines           = Chunk(UUID.randomUUID().toString, UUID.randomUUID().toString)
          _              <- Files.writeLines(sourcePath, lines)
          stream          = ZStream(sourcePath)
          destinationDir <- tempDir
          newFilename     = UUID.randomUUID().toString
          destinationPath = Path(destinationDir.toString(), newFilename)
          sink            = FileConnector.moveFile(_ => destinationPath)
          _              <- (stream >>> sink).exit
          linesInNewFile <- ZStream
                              .fromPath(destinationPath.toFile.toPath)
                              .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
                              .runCollect
        } yield assert(linesInNewFile)(equalTo(lines))
      }
    )

  lazy val tempFile: ZIO[Scope, Throwable, Path] =
    Files.createTempFileScoped(UUID.randomUUID().toString, None, List.empty)

  lazy val tempDir: ZIO[Scope, Throwable, Path] =
    Files.createTempDirectoryScoped(Some(UUID.randomUUID().toString), List.empty)

}
