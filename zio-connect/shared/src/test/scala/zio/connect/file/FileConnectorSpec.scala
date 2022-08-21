package zio.connect.file

import zio.nio.connect.Files
import zio.nio.file.Path
import zio.stream.{ZPipeline, ZStream}
import zio.test.Assertion._
import zio.test.TestAspect.{flaky, withLiveClock}
import zio.test.{TestClock, ZIOSpecDefault, assert, assertTrue, assertZIO}
import zio.{Cause, Chunk, Duration, Schedule, Scope, ZIO}

import java.io.IOException
import java.nio.file.{DirectoryNotEmptyException, StandardOpenOption}
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
            path         <- tempFileJava
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
          path         <- tempFileJava
          failingStream = ZStream(1).mapZIO(_ => ZIO.fail(NonIOException))
          sink          = FileConnector.writeFile(path)
          r            <- (failingStream >>> sink).exit
        } yield r
        assertZIO(prog)(failsCause(equalTo(Cause.die(NonIOException))))
      },
      test("overwrites existing file") {
        for {
          path            <- tempFileJava
          existingContents = Chunk[Byte](4, 5, 6, 7)
          _               <- Files.writeBytes(path, existingContents)
          input            = Chunk[Byte](1, 2, 3)
          _               <- ZStream.fromChunk(input) >>> FileConnector.writeFile(path)
          actual          <- ZStream.fromPath(path).runCollect
        } yield assert(input)(equalTo(actual))
      },
      test("creates and writes to file") {
        for {
          path   <- tempFileJava
          _      <- Files.delete(path)
          input   = Chunk[Byte](1, 2, 3)
          _      <- ZStream.fromChunk(input) >>> FileConnector.writeFile(path)
          actual <- ZStream.fromPath(path).runCollect
        } yield assert(input)(equalTo(actual))
      }
    )

  private lazy val listDirSuite =
    suite("listDir")(
      test("fails when IOException") {
        val prog = for {
          dir   <- tempDirJava
          stream = FileConnector.listDir(dir)
          _ <- Files.delete(dir) // delete the directory to cause an IOException
          r <- stream.runDrain.exit
        } yield r
        assertZIO(prog)(failsWithA[IOException])
      },
      test("succeeds") {
        for {
          dir              <- tempDirJava
          stream            = FileConnector.listDir(dir)
          file1            <- Files.createTempFileInScoped(dir, prefix = Some(UUID.randomUUID().toString))
          file2            <- Files.createTempFileInScoped(dir, prefix = Some(UUID.randomUUID().toString))
          file3            <- Files.createTempFileInScoped(dir, prefix = Some(UUID.randomUUID().toString))
          createdFilesPaths = Chunk(file1, file2, file3).sorted
          r                <- stream.runCollect.map(files => files.sorted)
        } yield assert(createdFilesPaths)(equalTo(r))
      }
    )

  private lazy val readFileSuite =
    suite("readFile")(
      test("fails when IOException") {
        val prog = for {
          file  <- tempFileJava
          stream = FileConnector.readFile(file)
          _ <- Files.delete(file) // delete the file to cause an IOException
          r <- stream.runDrain.exit
        } yield r
        assertZIO(prog)(failsWithA[IOException])
      },
      test("succeeds") {
        for {
          file   <- tempFileJava
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
          file  <- tempFileJava
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
          parentDir <- tempDirJava
          file      <- tempFileInDir(parentDir)
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
          file  <- tempFileJava
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
          dir   <- tempDirJava
          file  <- tempFileInDir(dir)
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
            path <- tempFileJava
            _    <- Files.delete(path)
            r    <- (ZStream(path) >>> FileConnector.deleteFile).exit
          } yield r
        }
        assertZIO(prog)(failsWithA[IOException])
      },
      test("dies when non-IOException exception") {
        object NonIOException extends Throwable
        val prog = for {
          path         <- tempFileJava
          failingStream = ZStream(path).mapZIO(_ => ZIO.fail(NonIOException))
          r            <- (failingStream >>> FileConnector.deleteFile).exit
        } yield r
        assertZIO(prog)(failsCause(equalTo(Cause.die(NonIOException))))
      },
      test("delete file") {
        for {
          file        <- tempFileJava
          _           <- ZStream.succeed(file) >>> FileConnector.deleteFile
          fileDeleted <- Files.notExists(file)
        } yield assert(fileDeleted)(equalTo(true))
      },
      test("delete empty directory ") {
        for {
          sourceDir          <- tempDirJava
          _                  <- (ZStream(sourceDir) >>> FileConnector.deleteFile).exit
          directoryIsDeleted <- Files.notExists(sourceDir)
        } yield assertTrue(directoryIsDeleted)
      },
      test("fails for directory not empty") {
        val prog = for {
          sourceDir <- tempDirJava
          _         <- tempFileInDir(sourceDir)

          r <- (ZStream(sourceDir) >>> FileConnector.deleteFile).exit
        } yield r
        assertZIO(prog)(fails(isSubtype[DirectoryNotEmptyException](anything)))
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
      test("move a file") {
        for {
          sourcePath     <- tempFile
          lines           = Chunk(UUID.randomUUID().toString, UUID.randomUUID().toString)
          _              <- Files.writeLines(sourcePath.toFile.toPath, lines)
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
          sourceIsDeleted <- Files.notExists(sourcePath.toFile.toPath)
        } yield assertTrue(sourceIsDeleted) && assert(linesInNewFile)(equalTo(lines))
      },
      test("move a directory") {
        for {
          sourceDir     <- tempDir
          sourceFile    <- tempFileInDir(sourceDir.toFile.toPath)
          sourceFileName = sourceFile.toFile.getName
          lines          = Chunk(UUID.randomUUID().toString, UUID.randomUUID().toString)
          _             <- Files.writeLines(sourceFile, lines)

          destinationDir    <- tempDir
          newDirname         = UUID.randomUUID().toString
          destinationDirPath = Path(destinationDir.toString(), newDirname)

          _ <- (ZStream(sourceDir) >>> FileConnector.moveFile(_ => destinationDirPath)).exit

          targetChildren <- ZIO
                              .attempt(destinationDirPath.toFile.listFiles())
          destinationFileName = targetChildren.headOption.map(_.getName)
          linesInNewFile <- targetChildren.headOption match {
                              case Some(f) =>
                                ZStream
                                  .fromPath(f.toPath)
                                  .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
                                  .runCollect
                              case None => ZIO.succeed(Chunk.empty)
                            }

          originalDirectoryIsDeleted <- Files.notExists(sourceDir.toFile.toPath)
        } yield assertTrue(originalDirectoryIsDeleted) &&
          assertTrue(targetChildren.size == 1) &&
          assert(destinationFileName)(isSome(containsString(sourceFileName))) &&
          assert(linesInNewFile)(equalTo(lines))
      }
    )

  val tempFileJava: ZIO[Scope with java.nio.file.FileSystem, Throwable, java.nio.file.Path] =
    ZIO.acquireRelease(for {
      fs <- ZIO.service[java.nio.file.FileSystem]
      p  <- ZIO.attempt(fs.getPath(UUID.randomUUID().toString))
      r  <- ZIO.attempt(java.nio.file.Files.createFile(p))
    } yield r)(p => ZIO.attempt(java.nio.file.Files.deleteIfExists(p)).orDie)

  lazy val tempFile: ZIO[Scope with Files, Throwable, Path] =
    Files.createTempFileScoped(UUID.randomUUID().toString, None, List.empty)

  lazy val tempDirJava: ZIO[Scope with java.nio.file.FileSystem, Throwable, java.nio.file.Path] =
    ZIO.acquireRelease(for {
      fs <- ZIO.service[java.nio.file.FileSystem]
      p  <- ZIO.attempt(fs.getPath(UUID.randomUUID().toString))
      r  <- ZIO.attempt(java.nio.file.Files.createDirectory(p))
    } yield r)(p => ZIO.attempt(java.nio.file.Files.deleteIfExists(p)).orDie)

  lazy val tempDir: ZIO[Scope with Files, Throwable, Path] =
    Files.createTempDirectoryScoped(Some(UUID.randomUUID().toString), List.empty)

  def tempFileInDir(dir: java.nio.file.Path): ZIO[Scope with Files, Throwable, java.nio.file.Path] =
    Files.createTempFileInScoped(dir, UUID.randomUUID().toString, None, List.empty)

}
