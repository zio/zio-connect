import zio._
import zio.connect.file._
import zio.stream._

import java.io.{ByteArrayInputStream, IOException}
import java.nio.charset.StandardCharsets

object Example1 extends ZIOAppDefault {

  /**
   * Checks if the content of the file is equal to the given string.
   * Notes: Many of the methods in the file connector api create sinks, wrapping stream effects in [[ZSink.fromZIO]]
   * will prove useful
   * @return
   */
  def checkWrittenContent: ZSink[FileConnector with Scope, IOException, Byte, Nothing, Boolean] =
    for {
      dir    <- tempDirPath
      exists <- existsPath(dir)
      file   <- if (exists) tempPathIn(dir) else ZSink.fail(new IOException(s"path ${dir.toString} doesn't exist"))
      _      <- ZSink.fromZIO(contentStream >>> writePath(file))
      read   <- ZSink.fromZIO(readPath(file) >>> ZPipeline.utf8Decode >>> contentSink)
    } yield read == content

  // We need something to trigger our sink, in practice though you will likely have a stream of bytes to write
  // and consume
  val program = ZStream.succeed(1.toByte) >>> checkWrittenContent

  override def run: ZIO[Any with ZIOAppArgs with Scope, Any, Any] =
    program
      .provideSome[Scope](zio.connect.file.live)
      .tap(equal => Console.printLine(s"content is equal: $equal"))

  val content: String =
    """
      |I'm your phantom dance partner. I'm your shadow. I'm not anything more.
      |""".stripMargin

  val contentStream = ZStream.fromInputStream(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)))
  val contentSink   = ZSink.mkString
}
