package zio.connect.file

import zio.Scope
import zio.nio.connect.WatchServiceLayers
import zio.test.{Annotations, Live, TestConfig}

object LiveFileConnectorSpec extends FileConnectorSpec {

  override def spec =
    suite("LiveFileConnectorSpec")(fileConnectorSpec)
      .provideSome[Scope with Live with Annotations with TestConfig](
        FileOps.live,
        WatchServiceLayers.live,
        LiveFileConnector.layer
      )

}
