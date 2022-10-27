import zio._
import zio.connect.file._
import zio.stream._

import java.io.{ByteArrayInputStream, IOException}
import java.nio.charset.StandardCharsets

object Example1 extends ZIOAppDefault {

  /**
   * Checks if the content of the file is equal to the given string.
   * Notes: Many of the methods in the file connector api create streams, wrapping stream effects in [[ZStream.fromZIO]]
   * will prove useful
   */
  def checkWrittenContent: ZStream[FileConnector, IOException, Boolean] =
    for {
      dir    <- tempDirPath
      exists <- ZStream.fromZIO(ZStream(dir) >>> existsPath)
      file   <- if (exists) tempPathIn(dir) else ZStream.fail(new IOException(s"path ${dir.toString} doesn't exist"))
      _      <- ZStream.fromZIO(contentStream >>> writePath(file))
      read   <- ZStream.fromZIO(readPath(file) >>> ZPipeline.utf8Decode >>> contentSink)
    } yield read == content

  // We need something to trigger our sink, in practice though you will likely have a stream of bytes to write
  // and consume
  val program = checkWrittenContent.runCollect.map(_.headOption)

  override def run: ZIO[ZIOAppArgs with Scope, Any, Any] =
    program
      .provide(zio.connect.file.fileConnectorLiveLayer)
      .tap(equal => Console.printLine(s"content is equal: ${equal.contains(true)}"))

  val content: String =
    """
      |I'm your phantom dance partner. I'm your shadow. I'm not anything more.
      |""".stripMargin

  val contentStream = ZStream.fromInputStream(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)))
  val contentSink   = ZSink.mkString
}
