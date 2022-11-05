package zio.connect.ftp

import zio._
import zio.connect.ftp.FtpConnector.PathName
import zio.stream.ZPipeline.utf8Decode

import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._

import java.io.IOException

trait FtpConnectorSpec extends ZIOSpecDefault {

  val ftpConnectorSpec: Spec[Scope & FtpConnector, IOException] =
    statSuite + rmSuite + rmDirSuite + mkDirSuite + lsSuite + lsDescendantSuite + readFileSuite +  uploadSuite

  private lazy val statSuite: Spec[Scope & FtpConnector, IOException] =
    suite("stat")(
      test("returns None when path doesn't exist") {
        val path = PathName("/fail-stat.txt")
        for {
          content <- ZStream.succeed(path) >>> stat
        } yield assertTrue(content.isEmpty)
      },
      test("succeeds when file does exist") {
        val path = PathName("/succeed-stat.txt")
        val data = ZStream.fromChunks(Chunk.fromArray("hello".getBytes))
        for {
          _           <- data >>> upload(path)
          resource    <- ZStream.succeed(path) >>> stat
          fileDeleted <- (ZStream.succeed(path) >>> rm).as(true)
          resourceHasSamePath = resource.get.path == path
          resourceIsFile = !resource.get.isDirectory.get
        } yield assertTrue(resourceHasSamePath) &&
          assertTrue(resourceIsFile) &&
          assertTrue(fileDeleted)
      },
      test("succeeds when directory does exist") {
        val path = PathName("/test")
        for {
          dirCreated          <- (ZStream.succeed(path) >>> mkDir).as(true)
          resource            <- ZStream.succeed(path) >>> stat
          dirDeleted          <- (ZStream.succeed(path) >>> rmDir).as(true)
          resourceHasSamePath = resource.get.path == path
          resourceIsDirectory = resource.get.isDirectory.get
        } yield assertTrue(dirCreated) && assertTrue(dirDeleted) &&
          assertTrue(resourceHasSamePath) && assertTrue(resourceIsDirectory)
      }
    )

  private lazy val rmSuite: Spec[Scope & FtpConnector, IOException] =
    suite("rm")(
      test("fails when path is invalid") {
        val path = PathName("/fail-rm.txt")
        for {
          invalid <- (ZStream.succeed(path) >>> rm)
            .foldCause(_.failureOption.map(_.getMessage).getOrElse(""), _ => "")
        } yield assertTrue(invalid == s"Path is invalid. Cannot delete file : $path")
      },
      test("succeeds") {
        val path = PathName("/succeed-rm.txt")
        val data = ZStream.fromChunks(Chunk.fromArray("hello".getBytes))
        for {
          _    <- data >>> upload(path)
          _    <- ZStream.succeed(path) >>> rm
          stat <- ZStream.succeed(path) >>> stat
        } yield assertTrue(stat.isEmpty)
      }
    )

  private lazy val rmDirSuite: Spec[FtpConnector, IOException] =
    suite("rmDir")(
      test("fails when directory doesn't exist") {
        val path = PathName("/invalid")
        for {
          invalid <- (ZStream.succeed(path) >>> rmDir)
            .foldCause(_.failureOption.map(_.getMessage).getOrElse(""), _ => "")
        } yield assertTrue(invalid == s"Path is invalid. Cannot delete directory : $path")
      },
      test("succeeds") {
        val path = PathName("/succeed")
        for {
          _ <- ZStream.succeed(path) >>> mkDir
          _ <- ZStream.succeed(path) >>> rmDir
          resource <- ZStream.succeed(path) >>> stat
        } yield assertTrue(resource.isEmpty)
      }
    )

  private lazy val mkDirSuite: Spec[Scope & FtpConnector, IOException] =
    suite("mkDir")(
      test("fails when path is invalid") {
        val path = PathName("fail-mkdir.txt")
        val data = ZStream.fromChunks(Chunk.fromArray("hello".getBytes))
        for {
          _           <- data >>> upload(path)
          invalid     <- (ZStream.succeed(path) >>> mkDir).foldCause(_.failureOption.map(_.getMessage).getOrElse(""), _ => "")
          fileDeleted <- (ZStream.succeed(path) >>> rm).as(true)
        } yield assertTrue(fileDeleted) &&
          assertTrue(invalid == s"Path is invalid. Cannot create directory : $path")
      },
      test("succeeds") {
        val path = PathName("succeed")
        for {
          _          <- ZStream.succeed(path) >>> mkDir
          resource   <- ZStream.succeed(path) >>> stat
          dirDeleted <- (ZStream.succeed(path) >>> rmDir).as(true)
        } yield
          assertTrue(dirDeleted) &&
          assertTrue(resource.get.isDirectory.get)
      }
    )

  private lazy val lsSuite: Spec[Scope & FtpConnector, IOException] =
    suite("ls")(
      test("fails with invalid directory") {
        val path = PathName("/invalid-path")
        for {
          files <- ls(path).runCollect
        } yield assert(files)(hasSameElements(Nil))
      },
      test("succeeds") {
        val dataPath = PathName("/hello.txt")
        val data = ZStream.fromChunks(Chunk.fromArray("hello".getBytes))

        for {
            _           <- data >>> upload(dataPath)
            files       <- ls(PathName("/")).runFold(List.empty[String])((l, resource) => l :+ resource.path)
            fileDeleted <- (ZStream(dataPath) >>> rm).as(true)
        } yield assertTrue(fileDeleted) &&
          assert(files)(hasSameElements(List(dataPath)))
      }
    )

  private lazy val lsDescendantSuite: Spec[Scope & FtpConnector, IOException] =
    suite("lsDescendant")(
      test("succeeds") {
        val dirPath = PathName("/dir1")
        val filePath = PathName("/hello.txt")
        val fileInDirPath = PathName(dirPath + filePath)
        val data = ZStream.fromChunks(Chunk.fromArray("hello".getBytes))

        for {
          _                <- data >>> upload(filePath)
          _                <- ZStream.succeed(dirPath) >>> mkDir
          _                <- data >>> upload(fileInDirPath)
          files            <- lsDescendant(PathName("/")).runFold(List.empty[String])((s, f) => f.path +: s)
          fileDeleted      <- (ZStream.succeed(filePath) >>> rm).as(true)
          fileInDirDeleted <- (ZStream.succeed(fileInDirPath) >>> rm).as(true)
          dirDeleted       <- (ZStream.succeed(dirPath) >>> rmDir).as(true)
        } yield assertTrue(fileDeleted) &&
          assertTrue(fileInDirDeleted) &&
          assertTrue(dirDeleted) &&
          assert(files.reverse)(hasSameElements(List(fileInDirPath, filePath)))
      },
      test("fails with invalid directory") {
        val path = PathName("/invalid-path-ls-descendant")
        for {
          files <- lsDescendant(path).runCollect
        } yield assertTrue(files == Chunk.empty)
      },
    )

  private lazy val readFileSuite: Spec[Scope & FtpConnector, IOException] =
    suite("readFile")(
      test("fails when file doesn't exist") {
        val path = PathName("/fail.txt")
        for {
          invalid <- readFile(path)
                      .via(utf8Decode)
                      .runCollect
                      .foldCause(_.failureOption.map(_.getMessage).getOrElse(""), _ => "")
        } yield assertTrue(invalid == s"File does not exist $path")
      },
      test("succeeds") {
        val path = PathName("/succeed.txt")
        val data = ZStream.fromChunk(Chunk.fromArray("hello world".getBytes))
        for {
          _           <- data >>> upload(path)
          content     <- readFile(path).via(utf8Decode).runCollect
          fileDeleted <- (ZStream.succeed(path) >>> rm).as(true)
        } yield assertTrue(fileDeleted) &&
          assert(content.mkString)(equalTo("hello world"))
      }
    )

  private lazy val uploadSuite: Spec[Scope & FtpConnector, IOException] =
    suite("upload")(
      test("fails when path is invalid") {
        val path = PathName("/dir/fail.txt")
        val data = ZStream.fromChunk(Chunk.fromArray("hello world".getBytes))
        for {
          invalid <- (data >>> upload(path)).foldCause(_.failureOption.map(_.getMessage).getOrElse(""), _ => "")
        } yield assertTrue(invalid == s"Path is invalid. Cannot upload data to : $path")
      },
      test("succeeds") {
        val data = ZStream.fromChunk(Chunk.fromArray("hello world".getBytes))
        val path = PathName("/succeed.txt")
        (
          for {
            _           <- data >>> upload(path)
            content     <- readFile(path).via(utf8Decode).runCollect
            fileDeleted <- (ZStream.succeed(path) >>> rm).as(true)
          } yield assertTrue(fileDeleted) &&
            assertTrue(content.mkString == "hello world")
        )
      }
    )
}
