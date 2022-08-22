package zio.connect.file

import zio.{Scope, ZLayer}
import zio.nio.connect.{Files, WatchServiceLayers}
import zio.test.{Annotations, Live, TestConfig}

import java.nio.file.FileSystems

object LiveFileConnectorSpec extends FileConnectorSpec {

  override def spec =
    suite("LiveFileConnectorSpec")(fileConnectorSpec)
      .provideSome[Scope with Live with Annotations with TestConfig](
        ZLayer.succeed(FileSystems.getDefault),
        WatchServiceLayers.live,
        Files.live,
        LiveFileConnector.layer
      )

}
