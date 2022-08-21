package zio.connect.file

import com.google.common.jimfs.{Configuration, Jimfs}
import zio.nio.connect.{Files, WatchServiceLayers}
import zio.test.{Annotations, Live, TestConfig}
import zio.{Scope, ZIO, ZLayer}

object InMemoryFileConnectorSpec extends FileConnectorSpec {

  override def spec =
    suite("InMemoryFileConnectorSpec")(fileConnectorSpec)
      .provideSome[Scope with Live with Annotations with TestConfig](
        ZLayer.succeed(Jimfs.newFileSystem(Configuration.forCurrentPlatform())),
        zFileSystem,
        WatchServiceLayers.inMemory,
        Files.inMemory,
        LiveFileConnector.layer
      )

  private val zFileSystem = ZLayer.fromZIO(
    for {
      fs <- ZIO.service[java.nio.file.FileSystem]
      r   = zio.nio.file.FileSystem.fromJava(fs)
    } yield r
  )
}
