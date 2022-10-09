---
id: quickstart_index
title: "Quick Start"
---

FileConnector
--------------
```
libraryDependencies += "dev.zio" %% "zio-connect-file" % "<version>"
```

Example
-------

```scala
import zio._
import zio.stream._
import zio.connect.file._

import java.io.{ByteArrayInputStream, File, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.Path

import scala.language.postfixOps
import java.nio.file.Files
import java.nio.file.Paths

object FileConnectorDemo extends ZIOAppDefault {
  val path = Files.createTempDirectory("zio-connect-file")

  def checkWrittenContent(path: Path): ZSink[FileConnector with Scope, IOException, Byte, Nothing, Boolean] =
    for {
      exists <- existsPath(path)
      file   <- if (exists) tempPathIn(path) else ZSink.fail(new IOException(s"path ${path.toString} doesn't exist"))
      _      <- ZSink.fromZIO {
                  contentStream >>> writePath(file) // reading and writing files from a stream creates a ZIO, thus the need to create a ZSink from it
                }
      read   <- ZSink.fromZIO(readPath(file) >>> ZPipeline.utf8Decode >>> contentSink)
    } yield read == content

  def createTailAndDeleteDirectory(path: Path) =
    for {
      tmp1   <- tempPathIn(path)
      tmp2   <- tempPathIn(path)
      fiber1 <- ZSink.fromZIO(takeAndDecode(1)(tailPath(tmp1, 1 seconds)).fork) // note we are forking here
      fiber2 <- ZSink.fromZIO(takeAndDecode(1)(tailPath(tmp2, 1 seconds)).fork)
      _      <- ZSink.fromZIO(listPath(path).foreach(p => (contentStream >>> writePath(p))))
      _      <- ZSink.fromZIO(fiber2.join.debug(s"tailed ${tmp2.getFileName().toString()}"))
      _      <- ZSink.fromZIO(fiber1.join.debug(s"tailed ${tmp1.getFileName().toString()}"))
      _      <- ZSink.fromZIO(
                  ZStream.succeed(path) >>> deleteRecursivelyPath // deletePath would fail here as the directory is not empty
                )
    } yield ()

  def createAndMoveFiles =
    for {
      dir1         <- tempDirPath
      dir2         <- tempDirPath
      file         <- tempFileIn(dir1.toFile())
      _            <- ZSink.fromZIO {
                        (ZStream.succeed(1.toByte) >>> existsFile(file)).debug(s"Does file exist?") // should be true
                      }
      _            <- ZSink.fromZIO(contentStream >>> writeFile(file))
      file2         = Paths.get(dir2.toString(), file.getName()).toFile()
      mover         = moveFile(_ => file2)
      _            <- ZSink.fromZIO(ZStream.succeed(file) >>> mover)
      _            <- ZSink.fromZIO {
                        (ZStream.succeed(1.toByte) >>> existsFile(file)).debug(s"Does file exist now?") // should be false
                      }
      movedContent <- ZSink.fromZIO((readFile(file2) >>> ZPipeline.utf8Decode >>> contentSink))
    } yield movedContent == content

  val checkProgram = Console.printLine("checkProgram:") *> (ZStream.succeed(1.toByte) >>> checkWrittenContent(path))
    .debug("content equivalent?")

  val createAndDeleteProgram =
    Console.printLine("\ncreateAndDeleteProgram:") *> (ZStream.succeed(1.toByte) >>> createTailAndDeleteDirectory(path))

  val createAndMoveProgram =
    Console.printLine("\ncreateAndMoveProgram:") *> (ZStream.succeed(1.toByte) >>> createAndMoveFiles)
      .debug("Moved content equivalent?")

  override def run =
    (checkProgram *> createAndDeleteProgram *> createAndMoveProgram)
      .provideSome[Scope](zio.connect.file.live)
      .tap(_ => Console.printLine("Done!"))
      .orDie

  val content: String =
    """
      |I'm your phantom dance partner. I'm your shadow. I'm not anything more.
      |""".stripMargin

  val contentStream = ZStream.fromInputStream(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)))
  val contentSink   = ZSink.mkString

  def takeAndDecode[R](n: Int)(stream: ZStream[R, IOException, Byte]) =
    stream
      .via(ZPipeline.utf8Decode)
      .take(n)
      .run(contentSink)

}
```
