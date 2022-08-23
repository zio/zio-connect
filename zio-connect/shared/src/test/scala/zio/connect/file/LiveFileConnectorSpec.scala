package zio.connect.file

import zio.{Scope, ZLayer}
import zio.nio.connect.WatchServiceLayers
import zio.test.{Annotations, Live, TestConfig}

import java.nio.file.FileSystems

object LiveFileConnectorSpec extends FileConnectorSpec {

  override def spec =
    suite("LiveFileConnectorSpec")(fileConnectorSpec)
      .provideSome[Scope with Live with Annotations with TestConfig](
        ZLayer.succeed(FileSystems.getDefault),
        FileOps.liveFileOps,
        WatchServiceLayers.live,
        LiveFileConnector.layer
      )

}
