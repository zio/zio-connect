package zio.connect.file

import zio.stream.{ZPipeline, ZSink, ZStream}
import zio.test.Assertion._
import zio.test.{TestClock, ZIOSpecDefault, assert, assertZIO}
import zio.{Cause, Chunk, Duration, Queue, Schedule, ZIO}

import java.io.IOException

trait FileConnectorSpec extends ZIOSpecDefault {

  val fileConnectorSpec =
    writeSuite + listSuite + readSuite +
      tailSuite + tailUsingWatchServiceSuite +
      deleteSuite + moveSuite

  private lazy val writeSuite =
    suite("writePath")(
      test("fails when IOException") {
        val ioException: IOException = new IOException("test ioException")
        val sink                     = FileConnector.tempPath.flatMap(path => FileConnector.writePath(path))
        val failingStream            = ZStream(1).mapZIO(_ => ZIO.fail(ioException))
        val prog                     = (failingStream >>> sink).exit

        assertZIO(prog)(fails(equalTo(ioException)))
      },
      test("dies when non-IOException exception") {
        object NonIOException extends Throwable
        val sink          = FileConnector.tempPath.flatMap(path => FileConnector.writePath(path))
        val failingStream = ZStream(1).mapZIO(_ => ZIO.fail(NonIOException))
        val prog          = (failingStream >>> sink).exit

        assertZIO(prog)(failsCause(equalTo(Cause.die(NonIOException))))
      },
      test("overwrites existing file") {
        val existingContents = Chunk[Byte](4, 5, 6, 7)
        val input            = Chunk[Byte](1, 2, 3)

        val prog = for {
          path   <- FileConnector.tempPath
          _      <- ZSink.fromZIO(ZStream.fromChunk(existingContents) >>> FileConnector.writePath(path))
          _      <- ZSink.fromZIO(ZStream.fromChunk(input) >>> FileConnector.writePath(path))
          actual <- ZSink.fromZIO(ZStream.fromPath(path).runCollect)
        } yield assert(input)(equalTo(actual))

        ZStream(1.toByte) >>> prog
      },
      test("writes to file") {
        val input = Chunk[Byte](1, 2, 3)
        val prog = for {
          path   <- FileConnector.tempPath
          _      <- ZSink.fromZIO(ZStream.fromChunk(input) >>> FileConnector.writePath(path))
          actual <- ZSink.fromZIO(ZStream.fromPath(path).runCollect)
        } yield assert(input)(equalTo(actual))

        ZStream(1.toByte) >>> prog
      }
    )

  private lazy val listSuite =
    suite("listPath")(
      test("fails when IOException") {
        val prog = for {
          path <- FileConnector.tempPath
          _    <- ZSink.fromZIO(ZStream.succeed(path) >>> FileConnector.deletePath)
          _    <- ZSink.fromZIO(FileConnector.listPath(path).runDrain)
        } yield ()

        assertZIO((ZStream(1.toByte) >>> prog).exit)(failsWithA[IOException])
      },
      test("succeeds") {
        val prog = for {
          dir              <- FileConnector.tempDirPath
          path1            <- FileConnector.tempPathIn(dir)
          path2            <- FileConnector.tempPathIn(dir)
          path3            <- FileConnector.tempPathIn(dir)
          files            <- ZSink.fromZIO(FileConnector.listPath(dir).runCollect.map(_.sorted))
          createdFilesPaths = Chunk(path1, path2, path3).sorted
        } yield assert(createdFilesPaths)(equalTo(files))

        ZStream(1.toByte) >>> prog
      }
    )

  private lazy val readSuite =
    suite("readPath")(
      test("fails when IOException") {
        val prog = for {
          path <- FileConnector.tempPath
          _    <- ZSink.fromZIO(ZStream.succeed(path) >>> FileConnector.deletePath)
          _    <- ZSink.fromZIO(FileConnector.readPath(path).runDrain)
        } yield ()

        assertZIO((ZStream(1.toByte) >>> prog).exit)(failsWithA[IOException])
      },
      test("succeeds") {
        val content = Chunk[Byte](1, 2, 3)
        val prog = for {
          path   <- FileConnector.tempPath
          _      <- ZSink.fromZIO(ZStream.fromChunk(content) >>> FileConnector.writePath(path))
          actual <- ZSink.fromZIO(FileConnector.readPath(path).runCollect)
        } yield assert(content)(equalTo(actual))

        ZStream(1.toByte) >>> prog
      }
    )

  private lazy val tailSuite =
    suite("tailPath")(
      test("fails when IOException") {
        val prog = for {
          path  <- FileConnector.tempPath
          stream = FileConnector.tailPath(path, Duration.fromMillis(500))
          // delete the file to cause an IOException
          _     <- ZSink.fromZIO(ZStream.succeed(path) >>> FileConnector.deletePath)
          fiber <- ZSink.fromZIO(stream.runDrain.fork)
          _     <- ZSink.fromZIO(TestClock.adjust(Duration.fromMillis(3000)))
          r     <- ZSink.fromZIO(fiber.join)
        } yield r

        assertZIO((ZStream(1.toByte) >>> prog).exit)(failsWithA[IOException])
      },
      test("succeeds") {
        val str = s"test-value"

        val prog =
          for {
            parentDir  <- FileConnector.tempDirPath
            path       <- FileConnector.tempPathIn(parentDir)
            writeSink   = FileConnector.writePath(path)
            queue      <- ZSink.fromZIO(Queue.unbounded[Byte])
            queueStream = ZStream.fromQueue(queue)
            _ <- ZSink.fromZIO(
                   queue
                     .offerAll(str.getBytes ++ System.lineSeparator().getBytes)
                     .repeat(Schedule.recurs(3))
                     .fork
                 )
            _ <- ZSink.fromZIO((queueStream >>> writeSink).fork)
            fiber <- ZSink.fromZIO(
                       FileConnector
                         .tailPath(path, Duration.fromMillis(1000))
                         .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
                         .take(3)
                         .runCollect
                         .fork
                     )
            _ <- ZSink.fromZIO(
                   TestClock
                     .adjust(Duration.fromMillis(1000))
                     .repeat(Schedule.recurs(5))
                 )
            r <- ZSink.fromZIO(fiber.join)
          } yield assert(r)(equalTo(Chunk(str, str, str)))

        ZStream(1.toByte) >>> prog
      }
    )

  private lazy val tailUsingWatchServiceSuite =
    suite("tailPathUsingWatchService")(
      test("fails when IOException") {
        val prog = for {
          path  <- FileConnector.tempPath
          stream = FileConnector.tailPathUsingWatchService(path, Duration.fromMillis(500))
          // delete the file to cause an IOException
          _     <- ZSink.fromZIO(ZStream.succeed(path) >>> FileConnector.deletePath)
          fiber <- ZSink.fromZIO(stream.runDrain.fork)
          _     <- ZSink.fromZIO(TestClock.adjust(Duration.fromMillis(3000)))
          r     <- ZSink.fromZIO(fiber.join)
        } yield r

        assertZIO((ZStream(1.toByte) >>> prog).exit)(failsWithA[IOException])
      },
      test("succeeds") {
        val str = s"test-value"

        val prog =
          for {
            parentDir  <- FileConnector.tempDirPath
            path       <- FileConnector.tempPathIn(parentDir)
            writeSink   = FileConnector.writePath(path)
            queue      <- ZSink.fromZIO(Queue.unbounded[Byte])
            queueStream = ZStream.fromQueue(queue)
            _ <- ZSink.fromZIO(
                   queue
                     .offerAll(str.getBytes ++ System.lineSeparator().getBytes)
                     .repeat(Schedule.recurs(3))
                     .fork
                 )
            _ <- ZSink.fromZIO((queueStream >>> writeSink).fork)
            fiber <- ZSink.fromZIO(
                       FileConnector
                         .tailPathUsingWatchService(path, Duration.fromMillis(1000))
                         .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
                         .take(3)
                         .runCollect
                         .fork
                     )
            _ <- ZSink.fromZIO(
                   TestClock
                     .adjust(Duration.fromMillis(1000))
                     .repeat(Schedule.recurs(5))
                 )
            r <- ZSink.fromZIO(fiber.join)
          } yield assert(r)(equalTo(Chunk(str, str, str)))

        ZStream(1.toByte) >>> prog
      }
    )

  private lazy val deleteSuite =
    suite("deletePath")(
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

  private lazy val moveSuite =
    suite("movePath")(
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
