package zio.connect.file

import zio.Scope
import zio.nio.connect.WatchServiceLayers
import zio.test.Live

object LiveFileConnectorSpec extends FileConnectorSpec {

  override def spec =
    suite("LiveFileConnectorSpec")(fileConnectorSpec)
      .provideSome[Scope with Live](
        FileOps.live,
        WatchServiceLayers.live,
        LiveFileConnector.layer
      )

}
