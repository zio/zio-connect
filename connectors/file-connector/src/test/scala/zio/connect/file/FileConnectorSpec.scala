package zio.connect.file

import zio.stream.{ZPipeline, ZSink, ZStream}
import zio.test.Assertion._
import zio.test.TestAspect.withLiveClock
import zio.test.{Spec, TestAspect, TestClock, ZIOSpecDefault, assert, assertTrue, assertZIO}
import zio.{Cause, Chunk, Duration, Queue, Scope, ZIO}

import java.io.IOException
import java.nio.file.{DirectoryNotEmptyException, Path, Paths}
import java.util.UUID

trait FileConnectorSpec extends ZIOSpecDefault {

  val fileConnectorSpec: Spec[FileConnector with Scope, IOException] =
    deleteSuite + deleteRecursivelySuite + listSuite + moveSuite + readSuite +
      tailSuite + tailUsingWatchServiceSuite + writeSuite

  private lazy val deleteSuite =
    suite("deletePath")(
      test("fails when IOException") {
        val prog = {
          for {
            dirPath <- tempDirPath
            path    <- tempPathIn(dirPath)
            _       <- ZSink.fromZIO(ZStream(path).mapZIO(_ => ZIO.fail(new IOException())) >>> deletePath)
          } yield ()
        }
        assertZIO((ZStream(1.toByte) >>> prog).exit)(failsWithA[IOException])
      },
      test("dies when non-IOException exception") {
        object NonIOException extends Throwable
        val prog = for {
          path         <- tempPath
          failingStream = ZStream(path).mapZIO(_ => ZIO.fail(NonIOException))
          r            <- ZSink.fromZIO(failingStream >>> deleteFile)
        } yield r
        assertZIO((ZStream(1.toByte) >>> prog).exit)(failsCause(equalTo(Cause.die(NonIOException))))
      },
      test("delete file") {
        val prog = for {
          dirPath    <- tempDirPath
          path       <- tempPathIn(dirPath)
          _          <- ZSink.fromZIO(ZStream.succeed(path) >>> deletePath)
          files      <- ZSink.fromZIO(listPath(dirPath).runCollect)
          fileDeleted = !files.contains(path)
        } yield assert(fileDeleted)(equalTo(true))
        ZStream(1.toByte) >>> prog
      },
      test("delete empty directory") {
        val prog = for {
          parentDir          <- tempDirPath
          _                  <- ZSink.fromZIO((ZStream(parentDir) >>> deletePath).exit)
          directoryIsDeleted <- existsPath(parentDir).map(!_)
        } yield assertTrue(directoryIsDeleted)

        ZStream(1.toByte) >>> prog
      },
      test("fails for directory not empty") {
        val prog = for {
          sourceDir <- tempDirPath
          _         <- tempPathIn(sourceDir)
          r         <- ZSink.fromZIO(ZStream(sourceDir) >>> deletePath)
        } yield r
        assertZIO((ZStream(1.toByte) >>> prog).exit)(fails(isSubtype[DirectoryNotEmptyException](anything)))
      }
    )

  private lazy val deleteRecursivelySuite =
    suite("deleteRecursivelyPath")(
      test("fails when IOException") {
        val prog = {
          for {
            dirPath <- tempDirPath
            path    <- tempPathIn(dirPath)
            _ <- ZSink.fromZIO(
                   ZStream(path).mapZIO(_ => ZIO.fail(new IOException())) >>> deleteRecursivelyPath
                 )
          } yield ()
        }
        assertZIO((ZStream(1.toByte) >>> prog).exit)(failsWithA[IOException])
      },
      test("dies when non-IOException exception") {
        object NonIOException extends Throwable
        val prog = for {
          path         <- tempDirPath
          failingStream = ZStream(path).mapZIO(_ => ZIO.fail(NonIOException))
          r            <- ZSink.fromZIO(failingStream >>> deleteRecursivelyPath)
        } yield r
        assertZIO((ZStream(1.toByte) >>> prog).exit)(failsCause(equalTo(Cause.die(NonIOException))))
      },
      test("delete non empty directory") {
        val prog = for {
          dirPath    <- tempDirPath
          _          <- tempPathIn(dirPath)
          filesInDir <- ZSink.fromZIO(listPath(dirPath).runCollect)
          _          <- ZSink.fromZIO(ZStream.succeed(dirPath) >>> deleteRecursivelyPath)
          dirDeleted <- existsPath(dirPath).map(!_)
        } yield assertTrue(filesInDir.nonEmpty) && assertTrue(dirDeleted)
        ZStream(1.toByte) >>> prog
      },
      test("delete empty directory ") {
        val prog = for {
          parentDir          <- tempDirPath
          _                  <- ZSink.fromZIO((ZStream(parentDir) >>> deleteRecursivelyPath).exit)
          directoryIsDeleted <- existsPath(parentDir).map(!_)
        } yield assertTrue(directoryIsDeleted)

        ZStream(1.toByte) >>> prog
      },
      test("delete file") {
        val prog = for {
          path          <- tempPath
          _             <- ZSink.fromZIO(ZStream(path) >>> deleteRecursivelyPath)
          fileIsDeleted <- existsPath(path).map(!_)
        } yield assertTrue(fileIsDeleted)

        ZStream(1.toByte) >>> prog
      }
    )

  private lazy val listSuite =
    suite("listPath")(
      test("fails when IOException") {
        val prog = for {
          path <- tempPath
          _    <- ZSink.fromZIO(ZStream.succeed(path) >>> deletePath)
          _    <- ZSink.fromZIO(listPath(path).runDrain)
        } yield ()

        assertZIO((ZStream(1.toByte) >>> prog).exit)(failsWithA[IOException])
      },
      test("succeeds") {
        val prog = for {
          dir              <- tempDirPath
          path1            <- tempPathIn(dir)
          path2            <- tempPathIn(dir)
          path3            <- tempPathIn(dir)
          files            <- ZSink.fromZIO(listPath(dir).runCollect.map(_.sorted))
          createdFilesPaths = Chunk(path1, path2, path3).sorted
        } yield assert(createdFilesPaths)(equalTo(files))

        ZStream(1.toByte) >>> prog
      }
    )

  private lazy val moveSuite =
    suite("movePath")(
      test("fails when IOException") {
        val ioException: IOException = new IOException("test ioException")
        val prog = {
          for {
            path           <- tempFile
            newDir         <- tempDirPath
            destinationPath = Paths.get(newDir.toString, path.toString)
            failingStream   = ZStream(path).mapZIO(_ => ZIO.fail(ioException))
            sink            = movePath(_ => destinationPath)
            r              <- ZSink.fromZIO(failingStream >>> sink)
          } yield r
        }
        assertZIO((ZStream(1.toByte) >>> prog).exit)(fails(equalTo(ioException)))
      },
      test("dies when non-IOException exception") {
        object NonIOException extends Throwable
        val prog = {
          for {
            path           <- tempPath
            newDir         <- tempDirPath
            destinationPath = Paths.get(newDir.toString, path.toString)
            failingStream   = ZStream(path).mapZIO(_ => ZIO.fail(NonIOException))
            sink            = movePath(_ => destinationPath)
            r              <- ZSink.fromZIO(failingStream >>> sink)
          } yield r
        }
        assertZIO((ZStream.succeed(1.toByte) >>> prog).exit)(failsCause(equalTo(Cause.die(NonIOException))))
      },
      test("move a file") {
        val prog = for {
          sourcePath <- tempPath
          lines = Chunk(
                    UUID.randomUUID().toString,
                    UUID.randomUUID().toString
                  )
          _ <- ZSink.fromZIO(
                 ZStream
                   .fromIterable(lines.map(_ + System.lineSeparator()).map(_.getBytes).flatten)
                   .run(writePath(sourcePath))
               )
          stream          = ZStream(sourcePath)
          newFilename     = UUID.randomUUID().toString
          destinationDir <- tempDirPath
          destinationPath = Paths.get(destinationDir.toString, newFilename)
          sink            = movePath(_ => destinationPath)
          _              <- ZSink.fromZIO((stream >>> sink).exit)
          linesInNewFile <- ZSink.fromZIO(
                              readPath(destinationPath)
                                .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
                                .runCollect
                            )
          sourceIsDeleted <- existsPath(sourcePath).map(!_)
          _               <- ZSink.fromZIO(ZStream.from(destinationPath) >>> deletePath)
        } yield assertTrue(sourceIsDeleted) && assert(linesInNewFile)(equalTo(lines))

        ZStream(1.toByte) >>> prog
      },
      test("move a directory and its children") {
        val prog =
          for {
            sourceDir            <- tempDirPath
            lines                 = Chunk(UUID.randomUUID().toString, UUID.randomUUID().toString)
            sourceDir_file1      <- tempPathIn(sourceDir)
            sourceDir_dir1       <- tempDirPathIn(sourceDir)
            sourceDir_dir1_file1 <- tempPathIn(sourceDir_dir1)

            _ <-
              ZSink.fromZIO(
                ZStream
                  .fromIterable(lines.map(_ + System.lineSeparator()).map(_.getBytes).flatten) >>> writePath(
                  sourceDir_file1
                )
              )

            destinationDirRoot <- tempDirPath
            destinationDirRoot_destinationDir <-
              ZSink.fromZIO(
                ZIO.succeed(Paths.get(destinationDirRoot.toFile.getPath, UUID.randomUUID().toString))
              )
            _ <- ZSink.fromZIO(ZStream(sourceDir) >>> movePath(_ => destinationDirRoot_destinationDir))

            destinationDirRoot_destinationDir_Children <-
              ZSink.fromZIO(
                listPath(destinationDirRoot_destinationDir).runCollect
              )
            linesInMovedFile <-
              ZSink.fromZIO(
                destinationDirRoot_destinationDir_Children.find(_.getFileName == sourceDir_file1.getFileName) match {
                  case Some(f) =>
                    readPath(f)
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
                    listPath(value).runCollect.orDie
                  )
                case None => ZSink.succeed(Chunk.empty[Path])
              }
            originalDirectoryIsDeleted <- existsPath(sourceDir).map(!_)
            _ <- ZSink.fromZIO(
                   ZStream.succeed(destinationDirRoot_destinationDir) >>> deleteRecursivelyPath
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

  private lazy val readSuite =
    suite("readPath")(
      test("fails when IOException") {
        val prog = for {
          path <- tempPath
          _    <- ZSink.fromZIO(ZStream.succeed(path) >>> deletePath)
          _    <- ZSink.fromZIO(readPath(path).runDrain)
        } yield ()

        assertZIO((ZStream(1.toByte) >>> prog).exit)(failsWithA[IOException])
      },
      test("succeeds") {
        val content = Chunk[Byte](1, 2, 3)
        val prog = for {
          path   <- tempPath
          _      <- ZSink.fromZIO(ZStream.fromChunk(content) >>> writePath(path))
          actual <- ZSink.fromZIO(readPath(path).runCollect)
        } yield assert(content)(equalTo(actual))

        ZStream(1.toByte) >>> prog
      }
    )

  private lazy val tailSuite =
    suite("tailPath")(
      test("fails when IOException") {
        val prog = for {
          path <- tempPath
          // delete the file to cause an IOException
          _ <- ZSink.fromZIO(ZStream.succeed(path) >>> deletePath)
          fiber <- ZSink.fromZIO(
                     tailPath(path, Duration.fromMillis(500)).runDrain.fork
                   )
          _ <- ZSink.fromZIO(TestClock.adjust(Duration.fromMillis(30000)))
          r <- ZSink.fromZIO(fiber.join)
        } yield r

        assertZIO((ZStream(1.toByte) >>> prog).exit)(failsWithA[IOException])
      } @@ TestAspect.diagnose(Duration.fromSeconds(10)),
      test("succeeds") {
        val str = s"test-value"

        val prog =
          for {
            parentDir  <- tempDirPath
            path       <- tempPathIn(parentDir)
            writeSink   = writePath(path)
            queue      <- ZSink.fromZIO(Queue.unbounded[Byte])
            queueStream = ZStream.fromQueue(queue)
            _ <- ZSink.fromZIO(
                   queue
                     .offerAll(str.getBytes ++ System.lineSeparator().getBytes)
                     .repeatN(3)
                     .fork
                 )
            _ <- ZSink.fromZIO((queueStream >>> writeSink).fork)
            fiber <- ZSink.fromZIO(
                       tailPath(path, Duration.fromMillis(1000))
                         .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
                         .take(3)
                         .runCollect
                         .fork
                     )
            r <- ZSink.fromZIO(fiber.join)
          } yield assert(r)(equalTo(Chunk(str, str, str)))

        ZStream(1.toByte) >>> prog
      } @@ withLiveClock
    )

  private lazy val tailUsingWatchServiceSuite =
    suite("tailPathUsingWatchService")(
      test("fails when IOException") {
        val prog = for {
          path  <- tempPath
          stream = tailPathUsingWatchService(path, Duration.fromMillis(500))
          // delete the file to cause an IOException
          _     <- ZSink.fromZIO(ZStream.succeed(path) >>> deletePath)
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
            parentDir  <- tempDirPath
            path       <- tempPathIn(parentDir)
            writeSink   = writePath(path)
            queue      <- ZSink.fromZIO(Queue.unbounded[Byte])
            queueStream = ZStream.fromQueue(queue)
            _ <- ZSink.fromZIO(
                   queue
                     .offerAll(str.getBytes ++ System.lineSeparator().getBytes)
                     .delay(Duration.fromMillis(100))
                     .repeatN(50)
                     .fork
                 )
            _ <- ZSink.fromZIO((queueStream >>> writeSink).fork)
            fiber <- ZSink.fromZIO(
                       tailPathUsingWatchService(path, Duration.fromMillis(1000))
                         .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
                         .take(3)
                         .runCollect
                         .fork
                     )
            r <- ZSink.fromZIO(fiber.join)
          } yield assert(r)(equalTo(Chunk(str, str, str)))

        ZStream(1.toByte) >>> prog
      } @@ withLiveClock @@ TestAspect.diagnose(Duration.fromSeconds(10))
    )

  private lazy val writeSuite =
    suite("writePath")(
      test("fails when IOException") {
        val ioException: IOException = new IOException("test ioException")
        val sink                     = tempPath.flatMap(path => writePath(path))
        val failingStream            = ZStream(1).mapZIO[Any, IOException, Byte](_ => ZIO.fail(ioException))
        val prog                     = (failingStream >>> sink).exit

        assertZIO(prog)(fails(equalTo(ioException)))
      },
      test("dies when non-IOException exception") {
        object NonIOException extends Throwable
        val sink          = tempPath.flatMap(path => writePath(path))
        val failingStream = ZStream(1).mapZIO[Any, Throwable, Byte](_ => ZIO.fail(NonIOException))
        val prog          = (failingStream >>> sink).exit

        assertZIO(prog)(failsCause(equalTo(Cause.die(NonIOException))))
      },
      test("overwrites existing file") {
        val existingContents = Chunk[Byte](4, 5, 6, 7)
        val input            = Chunk[Byte](1, 2, 3)

        val prog = for {
          path   <- tempPath
          _      <- ZSink.fromZIO(ZStream.fromChunk(existingContents) >>> writePath(path))
          _      <- ZSink.fromZIO(ZStream.fromChunk(input) >>> writePath(path))
          actual <- ZSink.fromZIO(readPath(path).runCollect)
        } yield assert(input)(equalTo(actual))

        ZStream(1.toByte) >>> prog
      },
      test("writes to file") {
        val input = Chunk[Byte](1, 2, 3)
        val prog = for {
          path   <- tempPath
          _      <- ZSink.fromZIO(ZStream.fromChunk(input) >>> writePath(path))
          actual <- ZSink.fromZIO(readPath(path).runCollect)
        } yield assert(input)(equalTo(actual))

        ZStream(1.toByte) >>> prog
      }
    )
}
