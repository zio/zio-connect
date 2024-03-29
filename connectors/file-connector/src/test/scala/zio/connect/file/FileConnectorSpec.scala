package zio.connect.file

import zio.stream.{ZPipeline, ZStream}
import zio.test.Assertion._
import zio.test.TestAspect.withLiveClock
import zio.test.{Spec, TestAspect, TestClock, ZIOSpecDefault, assert, assertTrue, assertZIO}
import zio.{Cause, Chunk, Duration, Queue, Random, Scope, ZIO}

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
            _ <- ZStream.fromZIO(ZStream.fail(new IOException()) >>> deletePath)
          } yield ()
        }
        assertZIO(prog.runDrain.exit)(failsWithA[IOException])
      },
      test("dies when non-IOException exception") {
        object NonIOException extends Throwable
        val prog = for {
          path         <- tempPath
          failingStream = ZStream.fail(NonIOException)
          r            <- ZStream.fromZIO(failingStream >>> deleteFile)
        } yield r
        assertZIO(prog.runDrain.exit)(failsCause(equalTo(Cause.die(NonIOException))))
      },
      test("delete file") {
        val prog = for {
          dirPath    <- tempDirPath
          path       <- tempPathIn(dirPath)
          _          <- ZStream.fromZIO(ZStream.succeed(path) >>> deletePath)
          files      <- ZStream.fromZIO(listPath(dirPath).runCollect)
          fileDeleted = !files.contains(path)
        } yield assert(fileDeleted)(equalTo(true))
        prog.runCollect.map(_.head)
      },
      test("delete empty directory") {
        val prog = for {
          parentDir          <- tempDirPath
          _                  <- ZStream.fromZIO((ZStream(parentDir) >>> deletePath).exit)
          directoryIsDeleted <- ZStream.fromZIO((ZStream(parentDir) >>> existsPath).map(!_))
        } yield assertTrue(directoryIsDeleted)

        prog.runCollect.map(_.head)
      },
      test("fails for directory not empty") {
        val prog = for {
          sourceDir <- tempDirPath
          _         <- tempPathIn(sourceDir)
          r         <- ZStream.fromZIO(ZStream(sourceDir) >>> deletePath)
        } yield r
        assertZIO(prog.runDrain.exit)(fails(isSubtype[DirectoryNotEmptyException](anything)))
      }
    )

  private lazy val deleteRecursivelySuite =
    suite("deleteRecursivelyPath")(
      test("fails when IOException") {
        val prog = {
          for {
            dirPath <- tempDirPath
            path    <- tempPathIn(dirPath)
            _ <- ZStream.fromZIO(
                   ZStream(path).mapZIO(_ => ZIO.fail(new IOException())) >>> deletePathRecursively
                 )
          } yield ()
        }
        assertZIO(prog.runDrain.exit)(failsWithA[IOException])
      },
      test("dies when non-IOException exception") {
        object NonIOException extends Throwable
        val prog = for {
          path         <- tempDirPath
          failingStream = ZStream.fail(NonIOException)
          r            <- ZStream.fromZIO(failingStream >>> deletePathRecursively)
        } yield r
        assertZIO(prog.runDrain.exit)(failsCause(equalTo(Cause.die(NonIOException))))
      },
      test("delete non empty directory") {
        val prog = for {
          dirPath    <- tempDirPath
          _          <- tempPathIn(dirPath)
          filesInDir <- ZStream.fromZIO(listPath(dirPath).runCollect)
          _          <- ZStream.fromZIO(ZStream.succeed(dirPath) >>> deletePathRecursively)
          dirDeleted <- ZStream.fromZIO((ZStream(dirPath) >>> existsPath).map(!_))
        } yield assertTrue(filesInDir.nonEmpty) && assertTrue(dirDeleted)
        prog.runCollect.map(_.head)
      },
      test("delete empty directory ") {
        val prog = for {
          parentDir          <- tempDirPath
          _                  <- ZStream.fromZIO((ZStream(parentDir) >>> deletePathRecursively).exit)
          directoryIsDeleted <- ZStream.fromZIO((ZStream(parentDir) >>> existsPath).map(!_))
        } yield assertTrue(directoryIsDeleted)

        prog.runCollect.map(_.head)
      },
      test("delete file") {
        val prog = for {
          path          <- tempPath
          _             <- ZStream.fromZIO(ZStream(path) >>> deletePathRecursively)
          fileIsDeleted <- ZStream.fromZIO((ZStream(path) >>> existsPath).map(!_))
        } yield assertTrue(fileIsDeleted)

        prog.runCollect.map(_.head)
      }
    )

  private lazy val listSuite =
    suite("listPath")(
      test("fails when IOException") {
        val prog = for {
          path <- tempPath
          _    <- ZStream.fromZIO(ZStream.succeed(path) >>> deletePath)
          _    <- ZStream.fromZIO(listPath(path).runDrain)
        } yield ()

        assertZIO(prog.runDrain.exit)(failsWithA[IOException])
      },
      test("succeeds") {
        val prog = for {
          dir              <- tempDirPath
          path1            <- tempPathIn(dir)
          path2            <- tempPathIn(dir)
          path3            <- tempPathIn(dir)
          files            <- ZStream.fromZIO(listPath(dir).runCollect.map(_.sorted))
          createdFilesPaths = Chunk(path1, path2, path3).sorted
        } yield assert(createdFilesPaths)(equalTo(files))

        prog.runCollect.map(_.head)
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
            failingStream   = ZStream.fail(ioException)
            sink            = movePath(_ => destinationPath)
            r              <- ZStream.fromZIO(failingStream >>> sink)
          } yield r
        }
        assertZIO(prog.runDrain.exit)(fails(equalTo(ioException)))
      },
      test("dies when non-IOException exception") {
        object NonIOException extends Throwable
        val prog = {
          for {
            path           <- tempPath
            newDir         <- tempDirPath
            destinationPath = Paths.get(newDir.toString, path.toString)
            failingStream   = ZStream.fail(NonIOException)
            sink            = movePath(_ => destinationPath)
            r              <- ZStream.fromZIO(failingStream >>> sink)
          } yield r
        }
        assertZIO(prog.runDrain.exit)(failsCause(equalTo(Cause.die(NonIOException))))
      },
      test("move a file") {
        val prog = for {
          sourcePath <- tempPath
          lines = Chunk(
                    UUID.randomUUID().toString,
                    UUID.randomUUID().toString
                  )
          _ <- ZStream.fromZIO(
                 ZStream
                   .fromIterable(lines.map(_ + System.lineSeparator()).map(_.getBytes).flatten)
                   .run(writePath(sourcePath))
               )
          stream          = ZStream(sourcePath)
          newFilename     = UUID.randomUUID().toString
          destinationDir <- tempDirPath
          destinationPath = Paths.get(destinationDir.toString, newFilename)
          sink            = movePath(_ => destinationPath)
          _              <- ZStream.fromZIO((stream >>> sink).exit)
          linesInNewFile <- ZStream.fromZIO(
                              readPath(destinationPath)
                                .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
                                .runCollect
                            )
          sourceIsDeleted <- ZStream.fromZIO((ZStream(sourcePath) >>> existsPath).map(!_))
          _               <- ZStream.fromZIO(ZStream.from(destinationPath) >>> deletePath)
        } yield assertTrue(sourceIsDeleted) && assert(linesInNewFile)(equalTo(lines))

        prog.runCollect.map(_.head)
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
              ZStream.fromZIO(
                ZStream
                  .fromIterable(lines.map(_ + System.lineSeparator()).map(_.getBytes).flatten) >>> writePath(
                  sourceDir_file1
                )
              )

            destinationDirRoot <- tempDirPath
            destinationDirRoot_destinationDir <-
              ZStream.fromZIO(
                ZIO.succeed(Paths.get(destinationDirRoot.toFile.getPath, UUID.randomUUID().toString))
              )
            _ <- ZStream.fromZIO(ZStream(sourceDir) >>> movePath(_ => destinationDirRoot_destinationDir))

            destinationDirRoot_destinationDir_Children <-
              ZStream.fromZIO(
                listPath(destinationDirRoot_destinationDir).runCollect
              )
            linesInMovedFile <-
              ZStream.fromZIO(
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
                  ZStream.fromZIO(
                    listPath(value).runCollect.orDie
                  )
                case None => ZStream.succeed(Chunk.empty[Path])
              }
            originalDirectoryIsDeleted <- ZStream.fromZIO((ZStream(sourceDir) >>> existsPath).map(!_))
            _ <- ZStream.fromZIO(
                   ZStream.succeed(destinationDirRoot_destinationDir) >>> deletePathRecursively
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

        prog.runCollect.map(_.head)
      }
    )

  private lazy val readSuite =
    suite("readPath")(
      test("fails when IOException") {
        val prog = for {
          path <- tempPath
          _    <- ZStream.fromZIO(ZStream.succeed(path) >>> deletePath)
          _    <- ZStream.fromZIO(readPath(path).runDrain)
        } yield ()

        assertZIO(prog.runDrain.exit)(failsWithA[IOException])
      },
      test("succeeds") {
        val content = Chunk[Byte](1, 2, 3)
        val prog = for {
          path   <- tempPath
          _      <- ZStream.fromZIO(ZStream.fromChunk(content) >>> writePath(path))
          actual <- ZStream.fromZIO(readPath(path).runCollect)
        } yield assert(content)(equalTo(actual))

        prog.runCollect.map(_.head)
      }
    )

  private lazy val tailSuite =
    suite("tailPath")(
      test("fails when IOException") {
        val prog = for {
          path <- tempPath
          // delete the file to cause an IOException
          _ <- ZStream.fromZIO(ZStream.succeed(path) >>> deletePath)
          fiber <- ZStream.fromZIO(
                     tailPath(path, Duration.fromMillis(500)).runDrain.fork
                   )
          _ <- ZStream.fromZIO(TestClock.adjust(Duration.fromMillis(30000)))
          r <- ZStream.fromZIO(fiber.join)
        } yield r

        assertZIO(prog.runDrain.exit)(failsWithA[IOException])
      } @@ TestAspect.diagnose(Duration.fromSeconds(10)),
      test("succeeds") {
        val str = s"test-value"

        val prog =
          for {
            parentDir  <- tempDirPath
            path       <- tempPathIn(parentDir)
            writeSink   = writePath(path)
            queue      <- ZStream.fromZIO(Queue.unbounded[Byte])
            queueStream = ZStream.fromQueue(queue)
            _ <- ZStream.fromZIO(
                   queue
                     .offerAll(str.getBytes ++ System.lineSeparator().getBytes)
                     .repeatN(3)
                     .fork
                 )
            _ <- ZStream.fromZIO((queueStream >>> writeSink).fork)
            fiber <- ZStream.fromZIO(
                       tailPath(path, Duration.fromMillis(1000))
                         .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
                         .take(3)
                         .runCollect
                         .fork
                     )
            r <- ZStream.fromZIO(fiber.join)
          } yield assert(r)(equalTo(Chunk(str, str, str)))

        prog.runCollect.map(_.head)
      } @@ withLiveClock
    )

  private lazy val tailUsingWatchServiceSuite =
    suite("tailPathUsingWatchService")(
      test("fails when IOException") {
        val prog = for {
          path  <- tempPath
          stream = tailPathUsingWatchService(path, Duration.fromMillis(500))
          // delete the file to cause an IOException
          _     <- ZStream.fromZIO(ZStream.succeed(path) >>> deletePath)
          fiber <- ZStream.fromZIO(stream.runDrain.fork)
          _     <- ZStream.fromZIO(TestClock.adjust(Duration.fromMillis(3000)))
          r     <- ZStream.fromZIO(fiber.join)
        } yield r

        assertZIO(prog.runDrain.exit)(failsWithA[IOException])
      },
      test("succeeds") {
        val str = s"test-value"

        val prog =
          for {
            parentDir  <- tempDirPath
            path       <- tempPathIn(parentDir)
            writeSink   = writePath(path)
            queue      <- ZStream.fromZIO(Queue.unbounded[Byte])
            queueStream = ZStream.fromQueue(queue)
            _ <- ZStream.fromZIO(
                   queue
                     .offerAll(str.getBytes ++ System.lineSeparator().getBytes)
                     .delay(Duration.fromMillis(100))
                     .repeatN(50)
                     .fork
                 )
            _ <- ZStream.fromZIO((queueStream >>> writeSink).fork)
            fiber <- ZStream.fromZIO(
                       tailPathUsingWatchService(path, Duration.fromMillis(1000))
                         .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
                         .take(3)
                         .runCollect
                         .fork
                     )
            r <- ZStream.fromZIO(fiber.join)
          } yield assert(r)(equalTo(Chunk(str, str, str)))

        prog.runCollect.map(_.head)
      } @@ withLiveClock @@ TestAspect.diagnose(Duration.fromSeconds(10))
    )

  private lazy val writeSuite =
    suite("writePath")(
      test("fails when IOException") {
        val ioException: IOException = new IOException("test ioException")

        val prog = for {
          path         <- tempPath
          sink          = writePath(path)
          failingStream = ZStream.fail(ioException)
          _            <- ZStream.fromZIO(failingStream >>> sink)
        } yield ()

        assertZIO(prog.runDrain.exit)(fails(equalTo(ioException)))
      },
      test("dies when non-IOException exception") {
        object NonIOException extends Throwable

        val prog = for {
          path         <- tempPath
          sink          = writePath(path)
          failingStream = ZStream.fail(NonIOException)
          _            <- ZStream.fromZIO(failingStream >>> sink)
        } yield ()

        assertZIO(prog.runDrain.exit)(failsCause(equalTo(Cause.die(NonIOException))))
      },
      test("overwrites existing file") {
        val existingContents = Chunk[Byte](4, 5, 6, 7)
        val input            = Chunk[Byte](1, 2, 3)

        val prog = for {
          path   <- tempPath
          _      <- ZStream.fromZIO(ZStream.fromChunk(existingContents) >>> writePath(path))
          _      <- ZStream.fromZIO(ZStream.fromChunk(input) >>> writePath(path))
          actual <- ZStream.fromZIO(readPath(path).runCollect)
        } yield assert(input)(equalTo(actual))

        prog.runCollect.map(_.head)
      },
      test("writes to file") {
        for {
          content <- Random.nextBytes(100000)
          actual <- (for {
                      path          <- tempPath
                      _             <- ZStream.fromZIO(ZStream.fromChunk(content) >>> writePath(path))
                      actualContent <- readPath(path)
                    } yield actualContent).runCollect
        } yield assert(content)(equalTo(actual))
      }
    )
}
