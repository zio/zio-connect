package zio.connect.file

import zio._
import zio.stream._
import zio.ZIOAppDefault
import zio.Console.printLine
import zio.nio.file.Path

object Example extends ZIOAppDefault {

  def run =
    FileConnector
      .tailFile(Path("/Users/brian/dev/zio/test.log"), 500.milliseconds)
      .via(ZPipeline.utf8Decode)
      .via(ZPipeline.splitLines)
      .tap(r => printLine(r))
      .runDrain
      .fold(
        e => {
          e.printStackTrace
          ExitCode.failure
        },
        _ => ExitCode.success
      )
      .provide(LiveFileConnector.layer)

}
