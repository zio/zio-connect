import zio._
import zio.connect.file._
import zio.stream._

import java.io.{ByteArrayInputStream, IOException}
import java.nio.charset.StandardCharsets
import scala.language.postfixOps

object Example2 extends ZIOAppDefault {

  /**
   * Create and tail files created in temp path and then delete (recursively as path is non-empty)
   */
  def createTailAndDeleteDirectory: ZStream[FileConnector, IOException, Unit] =
    for {
      path <- tempDirPath
      tmp1 <- tempPathIn(path)
      tmp2 <- tempPathIn(path)
      fiber1 <- ZStream.fromZIO(takeAndDecode(1)(tailPath(tmp1, 1 seconds)).fork) // note we are forking here
      fiber2 <- ZStream.fromZIO(takeAndDecode(1)(tailPath(tmp2, 1 seconds)).fork)
      _      <- ZStream.fromZIO(listPath(path).foreach(p => (contentStream >>> writePath(p))))
      _      <- ZStream.fromZIO(fiber1.join.debug(s"tailed ${tmp1.toString}"))
      _      <- ZStream.fromZIO(fiber2.join.debug(s"tailed ${tmp2.toString}"))
      _      <- ZStream.fromZIO(ZStream.succeed(path) >>> deletePathRecursively)
    } yield ()

  val program = createTailAndDeleteDirectory.runCollect

  override def run: ZIO[ZIOAppArgs with Scope, Any, Any] =
    program.provide(zio.connect.file.fileConnectorLiveLayer)

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
