package zio.connect.file

import zio.nio.file.{Path, Files => ZFiles}
import zio.stream.{ZPipeline, ZStream}
import zio.test.Assertion._
import zio.test.{TestClock, ZIOSpecDefault, assert, assertTrue, assertZIO}
import zio.{Cause, Chunk, Duration, Schedule, ZIO}

import java.io.IOException
import java.nio.file.{DirectoryNotEmptyException, StandardOpenOption, Files => JFiles}
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
            path         <- FileOps.tempFileScoped
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
          path         <- FileOps.tempFileScoped
          failingStream = ZStream(1).mapZIO(_ => ZIO.fail(NonIOException))
          sink          = FileConnector.writeFile(path)
          r            <- (failingStream >>> sink).exit
        } yield r
        assertZIO(prog)(failsCause(equalTo(Cause.die(NonIOException))))
      },
      test("overwrites existing file") {
        for {
          path            <- FileOps.tempFileScoped
          existingContents = Chunk[Byte](4, 5, 6, 7)
          _               <- ZFiles.writeBytes(Path.fromJava(path), existingContents)
          input            = Chunk[Byte](1, 2, 3)
          _               <- ZStream.fromChunk(input) >>> FileConnector.writeFile(path)
          actual          <- ZStream.fromPath(path).runCollect
        } yield assert(input)(equalTo(actual))
      },
      test("creates and writes to file") {
        for {
          path   <- FileOps.tempFileScoped
          _      <- ZFiles.delete(Path.fromJava(path))
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
          dir   <- FileOps.tempDirScoped
          stream = FileConnector.listDir(dir)
          _ <- ZFiles.delete(Path.fromJava(dir)) // delete the directory to cause an IOException
          r <- stream.runDrain.exit
        } yield r
        assertZIO(prog)(failsWithA[IOException])
      },
      test("succeeds") {
        for {
          dir              <- FileOps.tempDirScoped
          stream            = FileConnector.listDir(dir)
          file1            <- ZFiles.createTempFileInScoped(Path.fromJava(dir), prefix = Some(UUID.randomUUID().toString))
          file2            <- ZFiles.createTempFileInScoped(Path.fromJava(dir), prefix = Some(UUID.randomUUID().toString))
          file3            <- ZFiles.createTempFileInScoped(Path.fromJava(dir), prefix = Some(UUID.randomUUID().toString))
          createdFilesPaths = Chunk(file1.toString(), file2.toString(), file3.toString()).sorted
          r                <- stream.runCollect.map(files => files.sorted).map(_.map(_.toString))
        } yield assert(createdFilesPaths)(equalTo(r))
      }
    )

  private lazy val readFileSuite =
    suite("readFile")(
      test("fails when IOException") {
        val prog = for {
          file  <- FileOps.tempFileScoped
          stream = FileConnector.readFile(file)
          _ <- ZFiles.delete(Path.fromJava(file)) // delete the file to cause an IOException
          r <- stream.runDrain.exit
        } yield r
        assertZIO(prog)(failsWithA[IOException])
      },
      test("succeeds") {
        for {
          file   <- FileOps.tempFileScoped
          content = Chunk[Byte](1, 2, 3)
          _      <- ZFiles.writeBytes(Path.fromJava(file), content)
          stream  = FileConnector.readFile(file)
          r      <- stream.runCollect
        } yield assert(content)(equalTo(r))
      }
    )

  private lazy val tailFileSuite =
    suite("tailFile")(
      test("fails when IOException") {
        val prog = for {
          file  <- FileOps.tempFileScoped
          stream = FileConnector.tailFile(file, Duration.fromMillis(500))
          _ <- ZFiles.delete(Path.fromJava(file)) // delete the file to cause an IOException
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
          parentDir <- FileOps.tempDirScoped
          file      <- FileOps.tempFileInDirScoped(parentDir)
          _ <- ZFiles
                 .writeLines(Path.fromJava(file), List(str), openOptions = Set(StandardOpenOption.APPEND))
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
          file  <- FileOps.tempFileScoped
          stream = FileConnector.tailFileUsingWatchService(file, Duration.fromMillis(500))
          _ <- ZFiles.delete(Path.fromJava(file)) // delete the file to cause an IOException
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
          dir   <- FileOps.tempDirScoped
          file  <- FileOps.tempFileInDirScoped(dir)
          stream = FileConnector.tailFileUsingWatchService(file, Duration.fromMillis(500))
          fiber <- stream
                     .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
                     .take(3)
                     .runCollect
                     .fork
          _ <- ZFiles
                 .writeLines(Path.fromJava(file), List(str), openOptions = Set(StandardOpenOption.APPEND))
                 .repeat(Schedule.recurs(3) && Schedule.spaced(Duration.fromMillis(500)))
                 .fork
          _ <- TestClock.adjust(Duration.fromMillis(100)).repeat(Schedule.recurs(301)).fork
          r <- fiber.join.timeout(Duration.fromMillis(30000))
        } yield r
        assertZIO(prog)(equalTo(Some(Chunk(str, str, str))))
      }
    )

  private lazy val deleteFileSuite =
    suite("deleteFile")(
      test("fails when IOException") {
        val prog = {
          for {
            path <- FileOps.tempFileScoped
            _    <- ZFiles.delete(Path.fromJava(path))
            r    <- (ZStream(path) >>> FileConnector.deleteFile).exit
          } yield r
        }
        assertZIO(prog)(failsWithA[IOException])
      },
      test("dies when non-IOException exception") {
        object NonIOException extends Throwable
        val prog = for {
          path         <- FileOps.tempFileScoped
          failingStream = ZStream(path).mapZIO(_ => ZIO.fail(NonIOException))
          r            <- (failingStream >>> FileConnector.deleteFile).exit
        } yield r
        assertZIO(prog)(failsCause(equalTo(Cause.die(NonIOException))))
      },
      test("delete file") {
        for {
          file        <- FileOps.tempFileScoped
          _           <- ZStream.succeed(file) >>> FileConnector.deleteFile
          fileDeleted <- ZFiles.notExists(Path.fromJava(file))
        } yield assert(fileDeleted)(equalTo(true))
      },
      test("delete empty directory ") {
        for {
          sourceDir          <- FileOps.tempDirScoped
          _                  <- (ZStream(sourceDir) >>> FileConnector.deleteFile).exit
          directoryIsDeleted <- ZFiles.notExists(Path.fromJava(sourceDir))
        } yield assertTrue(directoryIsDeleted)
      },
      test("fails for directory not empty") {
        val prog = for {
          sourceDir <- FileOps.tempDirScoped
          _         <- FileOps.tempFileInDirScoped(sourceDir)

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
            path            <- FileOps.tempFileScoped
            newDir          <- FileOps.tempDirScoped
            destinationPath <- FileOps.getPath(newDir.toString, path.toString)
            failingStream    = ZStream(path).mapZIO(_ => ZIO.fail(ioException))
            sink             = FileConnector.moveFile(_ => destinationPath)
            r               <- (failingStream >>> sink).exit
          } yield r
        }
        assertZIO(prog)(fails(equalTo(ioException)))
      },
      test("dies when non-IOException exception") {
        object NonIOException extends Throwable
        val prog = {
          for {
            path            <- FileOps.tempFileScoped
            newDir          <- FileOps.tempDirScoped
            destinationPath <- FileOps.getPath(newDir.toString, path.getFileName.toString)
            failingStream    = ZStream(path).mapZIO(_ => ZIO.fail(NonIOException))
            sink             = FileConnector.moveFile(_ => destinationPath)
            r               <- (failingStream >>> sink).exit
          } yield r
        }
        assertZIO(prog)(failsCause(equalTo(Cause.die(NonIOException))))
      },
      test("move a file") {
        for {
          sourcePath      <- FileOps.tempFileScoped
          lines            = Chunk(UUID.randomUUID().toString, UUID.randomUUID().toString)
          _               <- ZFiles.writeLines(Path.fromJava(sourcePath), lines)
          stream           = ZStream(sourcePath)
          newFilename      = UUID.randomUUID().toString
          destinationDir  <- FileOps.tempDirScoped
          destinationPath <- FileOps.getPath(destinationDir.toString, newFilename)
          sink             = FileConnector.moveFile(_ => destinationPath)
          _               <- (stream >>> sink).exit
          linesInNewFile <- ZStream
                              .fromPath(destinationPath)
                              .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
                              .runCollect
          sourceIsDeleted <- ZFiles.notExists(Path.fromJava(sourcePath))
          _               <- ZFiles.delete(Path.fromJava(destinationPath))
        } yield assertTrue(sourceIsDeleted) && assert(linesInNewFile)(equalTo(lines))
      },
      test("move a directory with files") {
        for {
          sourceDir  <- FileOps.tempDirScoped
          lines       = Chunk(UUID.randomUUID().toString, UUID.randomUUID().toString)
          sourceFile <- FileOps.tempFileInDirScoped(sourceDir)
          _ <-
            ZFiles.writeLines(Path.fromJava(sourceFile), lines)

          destinationDirPath <- ZIO.acquireRelease(FileOps.getPath(UUID.randomUUID().toString))(p =>
                                  ZFiles.deleteIfExists(Path.fromJava(p)).orDie
                                )
          _ <-
            ZStream(sourceDir) >>> FileConnector.moveFile(_ => destinationDirPath)
          targetChildren <-
            ZIO.acquireRelease(
              ZStream
                .fromJavaStreamZIO(
                  ZIO.attempt(JFiles.list(destinationDirPath))
                )
                .runCollect
            )(children => ZIO.foreach(children)(file => ZFiles.deleteIfExists(Path.fromJava(file)).orDie))

          linesInNewFile <- targetChildren.headOption match {
                              case Some(f) =>
                                ZStream
                                  .fromPath(f)
                                  .via(
                                    ZPipeline.utf8Decode >>> ZPipeline.splitLines
                                  )
                                  .runCollect
                              case None =>
                                ZIO.succeed(Chunk.empty)
                            }
          sourceFileName              = sourceFile.getFileName.toString
          destinationFileName         = targetChildren.headOption.map(_.getFileName.toString)
          originalDirectoryIsDeleted <- ZFiles.notExists(Path.fromJava(sourceDir))
        } yield assertTrue(originalDirectoryIsDeleted) &&
          assertTrue(targetChildren.size == 1) &&
          assert(destinationFileName)(isSome(containsString(sourceFileName))) &&
          assert(linesInNewFile)(equalTo(lines))
      }
    )

}
