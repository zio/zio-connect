package zio.connect.file

import zio._

import zio.stream._

import java.nio.file._
import zio.ZIOAppDefault
import zio.Console.printLine

object Example extends ZIOAppDefault {

  def run = {
    FileConnector.tailFile(Paths.get("/Users/brian/dev/zio/test.log"), 500.milliseconds)
      .via(ZPipeline.utf8Decode)
      .via(ZPipeline.splitLines)
      .tap(r => printLine(r))
      .runDrain
      .fold(e => {
        e.printStackTrace
        ExitCode.failure
      }, _ => ExitCode.success)
      .provide(LiveFileConnector.layer)
  }

}
