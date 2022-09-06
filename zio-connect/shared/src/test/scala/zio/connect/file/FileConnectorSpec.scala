package zio.connect.file

import zio.stream.ZStream
import zio.test.Assertion._
import zio.test.{ZIOSpecDefault, assertZIO}
import zio.ZIO

import java.io.IOException

trait FileConnectorSpec extends ZIOSpecDefault {

  val fileConnectorSpec =
    writeFileSuite + listDirSuite + readFileSuite +
      tailFileSuite + tailFileUsingWatchServiceSuite +
      deleteFileSuite + moveFileSuite

  private lazy val writeFileSuite =
    suite("writeFile")(
      test("fails when IOException") {
        val ioException: IOException = new IOException("test ioException")
        val sink                     = FileConnector.tempPath.flatMap(path => FileConnector.writePath(path))
        val failingStream            = ZStream(1).mapZIO(_ => ZIO.fail(ioException))
        val prog                     = (failingStream >>> sink).exit
        assertZIO(prog)(fails(equalTo(ioException)))
      }
//      test("dies when non-IOException exception") {
//        object NonIOException extends Throwable
//        val prog = for {
//          path         <- Files.createTempFileScoped()
//          failingStream = ZStream(1).mapZIO(_ => ZIO.fail(NonIOException))
//          sink          = FileConnector.writeFile(path)
//          r            <- (failingStream >>> sink).exit
//        } yield r
//        assertZIO(prog)(failsCause(equalTo(Cause.die(NonIOException))))
//      },
//      test("overwrites existing file") {
//        for {
//          path            <- Files.createTempFileScoped()
//          existingContents = Chunk[Byte](4, 5, 6, 7)
//          _               <- Files.writeBytes(path, existingContents)
//          input            = Chunk[Byte](1, 2, 3)
//          _               <- ZStream.fromChunk(input) >>> FileConnector.writeFile(path)
//          actual          <- ZStream.fromPath(path.toFile.toPath).runCollect
//        } yield assert(input)(equalTo(actual))
//      },
//      test("creates and writes to file") {
//        for {
//          path   <- Files.createTempFileScoped()
//          _      <- Files.delete(path)
//          input   = Chunk[Byte](1, 2, 3)
//          _      <- ZStream.fromChunk(input) >>> FileConnector.writeFile(path)
//          actual <- ZStream.fromPath(path.toFile.toPath).runCollect
//        } yield assert(input)(equalTo(actual))
//      }
    )

  private lazy val listDirSuite =
    suite("listDir")(
//      test("fails when IOException") {
//        val prog = for {
//          dir   <- Files.createTempDirectoryScoped(None, List.empty)
//          stream = FileConnector.listDir(dir)
//          _ <- Files.delete(dir) // delete the directory to cause an IOException
//          r <- stream.runDrain.exit
//        } yield r
//        assertZIO(prog)(failsWithA[IOException])
//      },
//      test("succeeds") {
//        for {
//          dir              <- Files.createTempDirectoryScoped(None, List.empty)
//          stream            = FileConnector.listDir(dir)
//          file1            <- Files.createTempFileInScoped(dir, prefix = Some(UUID.randomUUID().toString))
//          file2            <- Files.createTempFileInScoped(dir, prefix = Some(UUID.randomUUID().toString))
//          file3            <- Files.createTempFileInScoped(dir, prefix = Some(UUID.randomUUID().toString))
//          createdFilesPaths = Chunk(file1.toFile.getPath, file2.toFile.getPath, file3.toFile.getPath).sorted
//          r                <- stream.runCollect.map(files => files.map(_.toFile.getPath).sorted)
//        } yield assert(createdFilesPaths)(equalTo(r))
//      }
    )

  private lazy val readFileSuite =
    suite("readFile")(
//      test("fails when IOException") {
//        val prog = for {
//          file  <- Files.createTempFileScoped()
//          stream = FileConnector.readFile(file)
//          _ <- Files.delete(file) // delete the file to cause an IOException
//          r <- stream.runDrain.exit
//        } yield r
//        assertZIO(prog)(failsWithA[IOException])
//      },
//      test("succeeds") {
//        for {
//          file   <- Files.createTempFileScoped()
//          content = Chunk[Byte](1, 2, 3)
//          _      <- Files.writeBytes(file, content)
//          stream  = FileConnector.readFile(file)
//          r      <- stream.runCollect
//        } yield assert(content)(equalTo(r))
//      }
    )

  private lazy val tailFileSuite =
    suite("tailFile")(
//      test("fails when IOException") {
//        val prog = for {
//          file  <- Files.createTempFileScoped()
//          stream = FileConnector.tailFile(file, Duration.fromMillis(500))
//          _ <- Files.delete(file) // delete the file to cause an IOException
//          fiber <- stream.runDrain.exit.fork
//          _ <- TestClock
//                 .adjust(Duration.fromMillis(3000))
//          r <- fiber.join
//        } yield r
//        assertZIO(prog)(failsWithA[IOException])
//      },
//      test("succeeds") {
//        val str = "test-value"
//        val prog = for {
//          parentDir <- Files.createTempDirectoryScoped(None, List.empty)
//          file      <- Files.createTempFileInScoped(parentDir)
//          _ <- Files
//                 .writeLines(file, List(str), openOptions = Set(StandardOpenOption.APPEND))
//                 .repeat(Schedule.recurs(3) && Schedule.spaced(Duration.fromMillis(1000)))
//                 .fork
//          stream = FileConnector.tailFile(file, Duration.fromMillis(1000))
//          fiber <- stream
//                     .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
//                     .take(3)
//                     .runCollect
//                     .fork
//          _ <- TestClock
//                 .adjust(Duration.fromMillis(3000))
//          r <- fiber.join
//        } yield r
//        assertZIO(prog)(equalTo(Chunk(str, str, str)))
//      }
    )

  private lazy val tailFileUsingWatchServiceSuite =
    suite("tailFileUsingWatchService")(
//      test("fails when IOException") {
//        val prog = for {
//          file  <- Files.createTempFileScoped()
//          stream = FileConnector.tailFileUsingWatchService(file, Duration.fromMillis(500))
//          _ <- Files.delete(file) // delete the file to cause an IOException
//          fiber <- stream.runDrain.exit.fork
//          _ <- TestClock
//                 .adjust(Duration.fromMillis(3000))
//          r <- fiber.join
//        } yield r
//        assertZIO(prog)(failsWithA[IOException])
//      },
//      test("succeeds") {
//        val str = "test-value"
//        val prog = for {
//          parentDir <- Files.createTempDirectoryScoped(None, List.empty)
//          file      <- Files.createTempFileInScoped(parentDir)
//          stream     = FileConnector.tailFileUsingWatchService(file, Duration.fromMillis(500))
//          fiber <- stream
//                     .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
//                     .take(3)
//                     .runCollect
//                     .fork
//          _ <- Files
//                 .writeLines(file, List(str), openOptions = Set(StandardOpenOption.APPEND))
//                 .repeat(Schedule.recurs(3) && Schedule.spaced(Duration.fromMillis(500)))
//                 .fork
//          _ <- TestClock.adjust(Duration.fromMillis(100)).repeat(Schedule.recurs(151)).fork
//          r <- fiber.join.timeout(Duration.fromMillis(15000))
//        } yield r
//        assertZIO(prog)(equalTo(Some(Chunk(str, str, str))))
//      }
    )

  private lazy val deleteFileSuite =
    suite("deleteFile")(
//      test("fails when IOException") {
//        val prog = {
//          for {
//            path <- Files.createTempFileScoped()
//            _    <- Files.delete(path)
//            r    <- (ZStream(path) >>> FileConnector.deleteFile).exit
//          } yield r
//        }
//        assertZIO(prog)(failsWithA[IOException])
//      },
//      test("dies when non-IOException exception") {
//        object NonIOException extends Throwable
//        val prog = for {
//          path         <- Files.createTempFileScoped()
//          failingStream = ZStream(path).mapZIO(_ => ZIO.fail(NonIOException))
//          r            <- (failingStream >>> FileConnector.deleteFile).exit
//        } yield r
//        assertZIO(prog)(failsCause(equalTo(Cause.die(NonIOException))))
//      },
//      test("delete file") {
//        for {
//          file        <- Files.createTempFileScoped()
//          _           <- ZStream.succeed(file) >>> FileConnector.deleteFile
//          fileDeleted <- Files.notExists(file)
//        } yield assert(fileDeleted)(equalTo(true))
//      },
//      test("delete empty directory ") {
//        for {
//          sourceDir          <- Files.createTempDirectoryScoped(None, List.empty)
//          _                  <- (ZStream(sourceDir) >>> FileConnector.deleteFile).exit
//          directoryIsDeleted <- Files.notExists(sourceDir)
//        } yield assertTrue(directoryIsDeleted)
//      },
//      test("fails for directory not empty") {
//        val prog = for {
//          sourceDir <- Files.createTempDirectoryScoped(None, List.empty)
//          _         <- Files.createTempFileInScoped(sourceDir)
//
//          r <- (ZStream(sourceDir) >>> FileConnector.deleteFile).exit
//        } yield r
//        assertZIO(prog)(fails(isSubtype[DirectoryNotEmptyException](anything)))
//      }
    )

  private lazy val moveFileSuite =
    suite("moveFile")(
//      test("fails when IOException") {
//        val ioException: IOException = new IOException("test ioException")
//        val prog = {
//          for {
//            path           <- Files.createTempFileScoped()
//            newDir         <- Files.createTempDirectoryScoped(None, List.empty)
//            destinationPath = Path(newDir.toString(), path.toFile.getName)
//            failingStream   = ZStream(path).mapZIO(_ => ZIO.fail(ioException))
//            sink            = FileConnector.moveFile(_ => destinationPath)
//            r              <- (failingStream >>> sink).exit
//          } yield r
//        }
//        assertZIO(prog)(fails(equalTo(ioException)))
//      },
//      test("dies when non-IOException exception") {
//        object NonIOException extends Throwable
//        val prog = {
//          for {
//            path           <- Files.createTempFileScoped()
//            newDir         <- Files.createTempDirectoryScoped(None, List.empty)
//            destinationPath = Path(newDir.toString(), path.toFile.getName)
//            failingStream   = ZStream(path).mapZIO(_ => ZIO.fail(NonIOException))
//            sink            = FileConnector.moveFile(_ => destinationPath)
//            r              <- (failingStream >>> sink).exit
//          } yield r
//        }
//        assertZIO(prog)(failsCause(equalTo(Cause.die(NonIOException))))
//      },
//      test("move a file") {
//        for {
//          sourcePath     <- Files.createTempFileScoped()
//          lines           = Chunk(UUID.randomUUID().toString, UUID.randomUUID().toString)
//          _              <- Files.writeLines(sourcePath, lines)
//          stream          = ZStream(sourcePath)
//          newFilename     = UUID.randomUUID().toString
//          destinationDir <- Files.createTempDirectoryScoped(None, List.empty)
//          destinationPath = Path(destinationDir.toString(), newFilename)
//          sink            = FileConnector.moveFile(_ => destinationPath)
//          _              <- (stream >>> sink).exit
//          linesInNewFile <- ZStream
//                              .fromPath(destinationPath.toFile.toPath)
//                              .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
//                              .runCollect
//          sourceIsDeleted <- Files.notExists(sourcePath)
//          _               <- Files.delete(destinationPath)
//        } yield assertTrue(sourceIsDeleted) && assert(linesInNewFile)(equalTo(lines))
//      },
//      test("move a directory with files") {
//        for {
//          sourceDir  <- Files.createTempDirectoryScoped(None, List.empty)
//          lines       = Chunk(UUID.randomUUID().toString, UUID.randomUUID().toString)
//          sourceFile <- Files.createTempFileInScoped(sourceDir)
//          _ <- Files.writeLines(sourceFile, lines)
//
//          tempDir  <- Files.createTempDirectoryScoped(None, List.empty)
//          destinationDirPath = Path(tempDir.toFile.getPath, UUID.randomUUID().toString)
//          _ <-
//            ZStream(sourceDir) >>> FileConnector.moveFile(_ => destinationDirPath)
//          targetChildren <-
//            ZIO.acquireRelease(Files.list(destinationDirPath).runCollect)(children =>
//              ZIO.foreach(children)(file => Files.deleteIfExists(file)).orDie
//            )
//
//          linesInNewFile <- targetChildren.headOption match {
//                              case Some(f) =>
//                                ZStream
//                                  .fromPath(f.toFile.toPath)
//                                  .via(
//                                    ZPipeline.utf8Decode >>> ZPipeline.splitLines
//                                  )
//                                  .runCollect
//                              case None =>
//                                ZIO.succeed(Chunk.empty)
//                            }
//          sourceFileName              = sourceFile.filename.toString
//          destinationFileName         = targetChildren.headOption.map(_.filename.toString)
//          originalDirectoryIsDeleted <- Files.notExists(sourceDir)
//        } yield assertTrue(originalDirectoryIsDeleted) &&
//          assertTrue(targetChildren.size == 1) &&
//          assert(destinationFileName)(isSome(containsString(sourceFileName))) &&
//          assert(linesInNewFile)(equalTo(lines))
//      }
    )

}
