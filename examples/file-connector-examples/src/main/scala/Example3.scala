import zio._
import zio.connect.file._
import zio.stream._

import java.io.{ByteArrayInputStream, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.Path

object Example3 extends ZIOAppDefault {

  def createMoveFileAndCheckContent: ZSink[FileConnector with Scope, IOException, Byte, Nothing, Boolean] =
    for {
      dir1 <- tempDirPath
      dir2 <- tempDirPath
      file <- tempFileIn(dir1.toFile)
      _ <- ZSink.fromZIO {
             (ZStream.succeed(1.toByte) >>> existsFile(file)).debug(s"Does file exist?") // should be true
           }
      _    <- ZSink.fromZIO(contentStream >>> writeFile(file))
      file2 = Path.of(dir2.toString, file.getName)
      mover = moveFile(_ => file2.toFile)
      _ <- ZSink.fromZIO(ZStream.succeed(file) >>> mover) // moves the file
      _ <- ZSink.fromZIO {
             (ZStream.succeed(1.toByte) >>> existsFile(file)).debug(s"Does file exist now?") // should be false
           }
      movedContent <- ZSink.fromZIO((readFile(file2.toFile) >>> ZPipeline.utf8Decode >>> contentSink))
    } yield movedContent == content

  val program = ZStream.succeed(1.toByte) >>> createMoveFileAndCheckContent

  override def run =
    program
      .provideSome[Scope](zio.connect.file.live)
      .tap(equal => Console.printLine(s"is moved content equivalent: $equal"))

  val content: String =
    """
      |I'm your phantom dance partner. I'm your shadow. I'm not anything more.
      |""".stripMargin

  val contentStream = ZStream.fromInputStream(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)))
  val contentSink   = ZSink.mkString

}
