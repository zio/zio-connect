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
        val path = PathName("/hello.txt")
        for {
          content <- ZStream.succeed(path) >>> stat
        } yield assertTrue(content.isEmpty)
      },
      test("succeeds when file does exist") {
        val path = PathName("/hello.txt")
        val data = ZStream.fromChunks(Chunk.fromArray("hello".getBytes))
        (
          for {
            _        <- data >>> upload(path)
            resource <- ZStream.succeed(path) >>> stat
            resourceHasSamePath = resource.get.path == path
            resourceIsFile = !resource.get.isDirectory.get
          } yield assertTrue(resourceHasSamePath) && assertTrue(resourceIsFile)
        ) <* (ZStream.succeed(path) >>> rm)
      },
      test("succeeds when directory does exist") {
        val path = PathName("/test")
        for {
          _ <- ZStream.succeed(path) >>> mkDir
          resource            <- ZStream.succeed(path) >>> stat
          resourceHasSamePath = resource.get.path == path
          resourceIsDirectory = resource.get.isDirectory.get
        } yield assertTrue(resourceHasSamePath) && assertTrue(resourceIsDirectory)
      }
    )

  private lazy val rmSuite: Spec[Scope & FtpConnector, IOException] =
    suite("rm")(
      test("fails when path is invalid") {
        val path = PathName("/invalid-path.txt")
        for {
          invalid <- (ZStream.succeed(path) >>> rm).foldCause(_.failureOption.map(_.getMessage).getOrElse(""), _ => "")
        } yield assertTrue(invalid == s"Path is invalid. Cannot delete file : $path")
      },
      test("succeeds") {
        val path = PathName("/hello-rm-suite.txt")
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
          invalid <- (ZStream.succeed(path) >>> rmDir).foldCause(_.failureOption.map(_.getMessage).getOrElse(""), _ => "")
        } yield assertTrue(invalid == s"Path is invalid. Cannot delete directory : $path")
      },
      test("succeeds") {
        val path = PathName("/rm-dir-suite")
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
        val path = PathName("/invalid-path-mk-dir.txt")
        val data = ZStream.fromChunks(Chunk.fromArray("hello".getBytes))
        for {
          _       <- data >>> upload(path)
          invalid <- (ZStream.succeed(path) >>> mkDir).foldCause(_.failureOption.map(_.getMessage).getOrElse(""), _ => "")
        } yield assertTrue(invalid == s"Path is invalid. Cannot create directory : $path")
      },
      test("succeeds") {
        val path = PathName("/valid-path-mk-dir-suite")
        for {
          _        <- ZStream.succeed(path) >>> mkDir
          resource <- ZStream.succeed(path) >>> stat
        } yield assertTrue(resource.get.isDirectory.get)
      }
    )

  private lazy val lsSuite: Spec[Scope & FtpConnector, IOException] =
    suite("ls")(
      test("fails with invalid directory") {
        val path = PathName("/invalid-path-ls")
        for {
          files <- ls(path).runFold(List.empty[String])((s, f) => f.path +: s)
        } yield assert(files.reverse)(hasSameElements(Nil))
      },
      test("succeeds") {
        val dataPath = PathName("/hello.txt")
        val dirPath = PathName("/dir")
        val data = ZStream.fromChunks(Chunk.fromArray("hello".getBytes))

        for {
            _     <- ZStream.succeed(dirPath) >>> mkDir
            _     <- data >>> upload(dataPath)
            files <- ls(PathName("/")).runFold(List.empty[String])((s, f) => f.path +: s)
        } yield assert(files.reverse)(hasSameElements(List("/hello.txt", "/dir")))
      }
    )

  private lazy val lsDescendantSuite: Spec[FtpConnector, IOException] =
    suite("lsDescendant")(
      test("succeeds") {
        for {
          files <- lsDescendant(PathName("/ls-desc-suite")).runFold(List.empty[String])((s, f) => f.path +: s)
        } yield assert(files.reverse)(hasSameElements(List("/notes.txt", "/dir1/users.csv", "/dir1/console.dump")))
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
        val path = PathName("/doesnt-exist.txt")
        for {
          invalid <- readFile(path)
                      .via(utf8Decode)
                      .runCollect
                      .foldCause(_.failureOption.map(_.getMessage).getOrElse(""), _ => "")
        } yield assertTrue(invalid == s"File does not exist $path")
      },
      test("succeeds") {
        val path = PathName("/hello-world.txt")
        val data = ZStream.fromChunks(Chunk.fromArray("hello world".getBytes))
        for {
          _       <- data >>> upload(path)
          content <- readFile(path).via(utf8Decode).runCollect
        } yield assert(content.mkString)(equalTo("hello world"))
      }
    )

  private lazy val uploadSuite: Spec[Scope & FtpConnector, IOException] =
    suite("upload")(
      test("fails when path is invalid") {
        val path = PathName("/invalid-path.txt")
        val data = ZStream.fromChunks(Chunk.fromArray("hello world".getBytes))
        for {
          invalid <- (data >>> upload(path)).foldCause(_.failureOption.map(_.getMessage).getOrElse(""), _ => "")
        } yield assertTrue(invalid == s"Path is invalid. Cannot upload data to : $path")
      },
      test("succeeds") {
        val data = ZStream.fromChunks(Chunk.fromArray("hello world".getBytes))
        val path = PathName("/succeed.txt")
        (
          for {
            _       <- data >>> upload(path)
            content <- readFile(path).via(utf8Decode).runCollect
          } yield assertTrue(content.mkString == "hello word")
        ) <* (ZStream.succeed(path) >>> rm)
      }
    )
}
