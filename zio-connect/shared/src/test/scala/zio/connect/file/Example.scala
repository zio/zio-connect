package zio.connect.file

import zio._
import zio.console._
import zio.stream._
import zio.duration._
import java.nio.file._

object Example extends App {

  def run(args: List[String]): ZIO[ZEnv, Nothing, ExitCode] = {
    FileConnector.Service.live.tailFile(Paths.get("/Users/brian/dev/zio/test.log"), 1.second)
      .aggregate(ZTransducer.utf8Decode)
      .aggregate(ZTransducer.splitLines)
      .tap(r => putStrLn(r))
      .runDrain
      .fold(e => {
        e.printStackTrace
        ExitCode.failure
      }, _ => ExitCode.success)
  }

}
