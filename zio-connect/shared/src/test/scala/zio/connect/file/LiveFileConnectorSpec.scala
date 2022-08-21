package zio.connect.file

import zio.{Scope, ZIO, ZLayer}
import zio.nio.connect.{Files, WatchServiceLayers}
import zio.test.{Annotations, Live, TestConfig}

import java.nio.file.FileSystems

object LiveFileConnectorSpec extends FileConnectorSpec {

  override def spec =
    suite("LiveFileConnectorSpec")(fileConnectorSpec)
      .provideSome[Scope with Live with Annotations with TestConfig](
        ZLayer.succeed(FileSystems.getDefault),
        zFileSystem,
        WatchServiceLayers.inMemory,
        Files.live,
        LiveFileConnector.layer
      )

  private val zFileSystem = ZLayer.fromZIO(
    for {
      fs <- ZIO.service[java.nio.file.FileSystem]
      r   = zio.nio.file.FileSystem.fromJava(fs)
    } yield r
  )
}
