package zio.connect.file

import zio.Scope
import zio.nio.connect.WatchServiceLayers

object LiveFileConnectorSpec extends FileConnectorSpec {

  override def spec =
    suite("LiveFileConnectorSpec")(fileConnectorSpec)
      .provideSome[Scope](
        FileOps.live,
        WatchServiceLayers.live,
        LiveFileConnector.layer
      )

}
