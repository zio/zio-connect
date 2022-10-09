import zio._
import zio.connect.file._
import zio.stream._

import java.io.{ByteArrayInputStream, IOException}
import java.nio.charset.StandardCharsets
import scala.language.postfixOps

object Example2 extends ZIOAppDefault {

  /**
   * Create and tail files created in temp path and then delete (recursively as path is non-empty)
   * @return
   */
  def createTailAndDeleteDirectory: ZSink[FileConnector with Scope, IOException, Byte, Nothing, Unit] =
    for {
      path <- tempDirPath
      tmp1 <- tempPathIn(path)
      tmp2 <- tempPathIn(path)
      fiber1 <- ZSink.fromZIO(takeAndDecode(1)(tailPath(tmp1, 1 seconds)).fork) // note we are forking here
      fiber2 <- ZSink.fromZIO(takeAndDecode(1)(tailPath(tmp2, 1 seconds)).fork)
      _      <- ZSink.fromZIO(listPath(path).foreach(p => (contentStream >>> writePath(p))))
      _      <- ZSink.fromZIO(fiber1.join.debug(s"tailed ${tmp1.toString}"))
      _      <- ZSink.fromZIO(fiber2.join.debug(s"tailed ${tmp2.toString}"))
      _      <- ZSink.fromZIO(ZStream.succeed(path) >>> deleteRecursivelyPath)
    } yield ()

  val program = ZStream.succeed(1.toByte) >>> createTailAndDeleteDirectory

  override def run: ZIO[Any with ZIOAppArgs with Scope, Any, Any] = program.provideSome[Scope](zio.connect.file.live)

  val content: String =
    """
      |I'm your phantom dance partner. I'm your shadow. I'm not anything more.
      |""".stripMargin

  val contentStream = ZStream.fromInputStream(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)))
  val contentSink   = ZSink.mkString

  def takeAndDecode[R](n: Int)(stream: ZStream[R, IOException, Byte]): ZIO[R, IOException, String] =
    stream
      .via(ZPipeline.utf8Decode)
      .take(n)
      .run(contentSink)
}
