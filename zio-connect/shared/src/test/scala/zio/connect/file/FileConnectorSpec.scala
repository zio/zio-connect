package zio.connect.file

import zio.stream.{ZPipeline, ZSink, ZStream}
import zio.test.Assertion._
import zio.test.{TestAspect, TestClock, ZIOSpecDefault, assert, assertTrue, assertZIO}
import zio.{Cause, Chunk, Duration, Queue, Schedule, ZIO}

import java.io.IOException
import java.nio.file.{DirectoryNotEmptyException, Path, Paths}
import java.util.UUID

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
                     .repeat(Schedule.recurs(3) && Schedule.spaced(Duration.fromMillis(1000)))
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
                     .adjust(Duration.fromMillis(60000))
                     .fork
                 )
            r <- ZSink.fromZIO(fiber.join)
          } yield assert(r)(equalTo(Chunk(str, str, str)))

        ZStream(1.toByte) >>> prog
      } @@ TestAspect.diagnose(Duration.fromSeconds(10))
    )

  private lazy val deleteSuite =
    suite("deletePath")(
      test("fails when IOException") {
        val prog = {
          for {
            dirPath <- FileConnector.tempDirPath
            path    <- FileConnector.tempPathIn(dirPath)
            _       <- ZSink.fromZIO(ZStream(path).mapZIO(_ => ZIO.fail(new IOException())) >>> FileConnector.deletePath)
          } yield ()
        }
        assertZIO((ZStream(1.toByte) >>> prog).exit)(failsWithA[IOException])
      },
      test("dies when non-IOException exception") {
        object NonIOException extends Throwable
        val prog = for {
          path         <- FileConnector.tempPath
          failingStream = ZStream(path).mapZIO(_ => ZIO.fail(NonIOException))
          r            <- ZSink.fromZIO(failingStream >>> FileConnector.deleteFile)
        } yield r
        assertZIO((ZStream(1.toByte) >>> prog).exit)(failsCause(equalTo(Cause.die(NonIOException))))
      },
      test("delete file") {
        val prog = for {
          dirPath    <- FileConnector.tempDirPath
          path       <- FileConnector.tempPathIn(dirPath)
          _          <- ZSink.fromZIO(ZStream.succeed(path) >>> FileConnector.deletePath)
          files      <- ZSink.fromZIO(FileConnector.listPath(dirPath).runCollect)
          fileDeleted = !files.contains(path)
        } yield assert(fileDeleted)(equalTo(true))
        ZStream(1.toByte) >>> prog
      },
      test("delete empty directory ") {
        val prog = for {
          parentDir         <- FileConnector.tempDirPath
          path              <- FileConnector.tempDirPathIn(parentDir)
          _                 <- ZSink.fromZIO((ZStream(path) >>> FileConnector.deletePath).exit)
          files             <- ZSink.fromZIO(FileConnector.listPath(parentDir).runCollect)
          directoryIsDeleted = !files.contains(path)
        } yield assertTrue(directoryIsDeleted)

        ZStream(1.toByte) >>> prog
      },
      test("fails for directory not empty") {
        val prog = for {
          sourceDir <- FileConnector.tempDirPath
          _         <- FileConnector.tempPathIn(sourceDir)
          r         <- ZSink.fromZIO(ZStream(sourceDir) >>> FileConnector.deletePath)
        } yield r
        assertZIO((ZStream(1.toByte) >>> prog).exit)(fails(isSubtype[DirectoryNotEmptyException](anything)))
      }
    )

  private lazy val moveSuite =
    suite("movePath")(
      test("fails when IOException") {
        val ioException: IOException = new IOException("test ioException")
        val prog = {
          for {
            path           <- FileConnector.tempFile
            newDir         <- FileConnector.tempDirPath
            destinationPath = Paths.get(newDir.toString, path.toString)
            failingStream   = ZStream(path).mapZIO(_ => ZIO.fail(ioException))
            sink            = FileConnector.movePath(_ => destinationPath)
            r              <- ZSink.fromZIO(failingStream >>> sink)
          } yield r
        }
        assertZIO((ZStream(1.toByte) >>> prog).exit)(fails(equalTo(ioException)))
      },
      test("dies when non-IOException exception") {
        object NonIOException extends Throwable
        val prog = {
          for {
            path           <- FileConnector.tempPath
            newDir         <- FileConnector.tempDirPath
            destinationPath = Paths.get(newDir.toString, path.toString)
            failingStream   = ZStream(path).mapZIO(_ => ZIO.fail(NonIOException))
            sink            = FileConnector.movePath(_ => destinationPath)
            r              <- ZSink.fromZIO(failingStream >>> sink)
          } yield r
        }
        assertZIO((ZStream.succeed(1.toByte) >>> prog).exit)(failsCause(equalTo(Cause.die(NonIOException))))
      },
      test("move a file") {
        val prog = for {
          sourcePath <- FileConnector.tempPath
          lines = Chunk(
                    UUID.randomUUID().toString,
                    UUID.randomUUID().toString
                  )
          _ <- ZSink.fromZIO(
                 ZStream
                   .fromIterable(lines.map(_ + System.lineSeparator()).map(_.getBytes).flatten)
                   .run(FileConnector.writePath(sourcePath))
               )
          stream          = ZStream(sourcePath)
          newFilename     = UUID.randomUUID().toString
          destinationDir <- FileConnector.tempDirPath
          destinationPath = Paths.get(destinationDir.toString, newFilename)
          sink            = FileConnector.movePath(_ => destinationPath)
          _              <- ZSink.fromZIO((stream >>> sink).exit)
          linesInNewFile <- ZSink.fromZIO(
                              ZStream
                                .fromPath(destinationPath.toFile.toPath)
                                .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
                                .runCollect
                            )
          sourceIsDeleted <- FileConnector.existsPath(sourcePath).map(!_)
          _               <- ZSink.fromZIO(ZStream.from(destinationPath) >>> FileConnector.deletePath)
        } yield assertTrue(sourceIsDeleted) && assert(linesInNewFile)(equalTo(lines))

        ZStream(1.toByte) >>> prog
      },
      test("move a directory and its children") {
        val prog =
          for {
            sourceDir            <- FileConnector.tempDirPath
            lines                 = Chunk(UUID.randomUUID().toString, UUID.randomUUID().toString)
            sourceDir_file1      <- FileConnector.tempPathIn(sourceDir)
            sourceDir_dir1       <- FileConnector.tempDirPathIn(sourceDir)
            sourceDir_dir1_file1 <- FileConnector.tempPathIn(sourceDir_dir1)

            _ <-
              ZSink.fromZIO(
                ZStream
                  .fromIterable(lines.map(_ + System.lineSeparator()).map(_.getBytes).flatten) >>> FileConnector
                  .writePath(
                    sourceDir_file1
                  )
              )

            destinationDirRoot <- FileConnector.tempDirPath
            destinationDirRoot_destinationDir <-
              ZSink.fromZIO(
                ZIO.succeed(Paths.get(destinationDirRoot.toFile.getPath, UUID.randomUUID().toString))
              )
            _ <- ZSink.fromZIO(ZStream(sourceDir) >>> FileConnector.movePath(_ => destinationDirRoot_destinationDir))

            destinationDirRoot_destinationDir_Children <-
              ZSink.fromZIO(
                FileConnector.listPath(destinationDirRoot_destinationDir).runCollect
              )
            linesInMovedFile <-
              ZSink.fromZIO(
                destinationDirRoot_destinationDir_Children.find(_.getFileName == sourceDir_file1.getFileName) match {
                  case Some(f) =>
                    ZStream
                      .fromPath(f.toFile.toPath)
                      .via(
                        ZPipeline.utf8Decode >>> ZPipeline.splitLines
                      )
                      .runCollect
                  case None =>
                    ZIO.succeed(Chunk.empty)
                }
              )

            destinationFileNames = destinationDirRoot_destinationDir_Children.map(_.getFileName)
            destinationDirRoot_destinationDir_dir1 =
              destinationDirRoot_destinationDir_Children.find(_.getFileName == sourceDir_dir1.getFileName)
            destinationDirRoot_destinationDir_dir1_Children <-
              destinationDirRoot_destinationDir_dir1 match {
                case Some(value) =>
                  ZSink.fromZIO(
                    FileConnector.listPath(value).runCollect.orDie
                  )
                case None => ZSink.succeed(Chunk.empty[Path])
              }
            originalDirectoryIsDeleted <- FileConnector.existsPath(sourceDir).map(!_)
            _ <- ZSink.fromZIO(
                   ZStream.succeed(destinationDirRoot_destinationDir) >>> FileConnector.deleteRecursivelyPath
                 )
          } yield assertTrue(originalDirectoryIsDeleted) &&
            assertTrue(destinationDirRoot_destinationDir_Children.size == 2) &&
            assert(destinationFileNames.sorted)(
              equalTo(Chunk(sourceDir_file1.getFileName, sourceDir_dir1.getFileName).sorted)
            ) &&
            assert(linesInMovedFile)(equalTo(lines)) &&
            assert(destinationDirRoot_destinationDir_dir1_Children.map(_.getFileName))(
              equalTo(Chunk(sourceDir_dir1_file1.getFileName))
            )

        ZStream(1.toByte) >>> prog
      }
    )

}
