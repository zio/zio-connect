import zio._
import zio.connect.file._
import zio.stream._

import java.io.{ByteArrayInputStream, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.Path

object Example3 extends ZIOAppDefault {

  def createMoveFileAndCheckContent: ZStream[FileConnector, IOException, Boolean] =
    for {
      dir1 <- tempDirPath
      dir2 <- tempDirPath
      file <- tempFileIn(dir1.toFile)
      _ <- ZStream.fromZIO {
             (ZStream(file) >>> existsFile).debug(s"Does file exist?") // should be true
           }
      _    <- ZStream.fromZIO(contentStream >>> writeFile(file))
      file2 = Path.of(dir2.toString, file.getName)
      mover = moveFile(_ => file2.toFile)
      _ <- ZStream.fromZIO(ZStream.succeed(file) >>> mover) // moves the file
      _ <- ZStream.fromZIO {
             (ZStream(file) >>> existsFile).debug(s"Does file exist now?") // should be false
           }
      movedContent <- ZStream.fromZIO((readFile(file2.toFile) >>> ZPipeline.utf8Decode >>> contentSink))
    } yield movedContent == content

  val program = createMoveFileAndCheckContent.runCollect

  override def run =
    program
      .provide(zio.connect.file.fileConnectorLiveLayer)
      .tap(equal => Console.printLine(s"is moved content equivalent: $equal"))

  val content: String =
    """
      |I'm your phantom dance partner. I'm your shadow. I'm not anything more.
      |""".stripMargin

  val contentStream = ZStream.fromInputStream(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)))
  val contentSink   = ZSink.mkString

}
