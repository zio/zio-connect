package zio.connect.fs2

import zio._

case class LiveFS2Connector() extends FS2Connector {}

object LiveFS2Connector {

  val layer: ZLayer[Any, Nothing, LiveFS2Connector] =
    ZLayer.fromFunction(LiveFS2Connector.apply _)

}
