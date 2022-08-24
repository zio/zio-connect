package zio.connect.file

import com.google.common.jimfs.{Configuration, Jimfs}
import zio.connect.file.FileConnectorSpec.zioFileSystem
import zio.nio.connect.WatchServiceLayers
import zio.test.{Annotations, Live, TestConfig}
import zio.{Scope, ZLayer}

object InMemoryFileConnectorSpec extends FileConnectorSpec {

  override def spec =
    suite("InMemoryFileConnectorSpec")(fileConnectorSpec)
      .provideSome[Scope with Live with Annotations with TestConfig](
        ZLayer.succeed(Jimfs.newFileSystem(Configuration.forCurrentPlatform())),
        zioFileSystem,
        FileOps.inMemory,
        WatchServiceLayers.inMemory,
        LiveFileConnector.layer
      )

}
