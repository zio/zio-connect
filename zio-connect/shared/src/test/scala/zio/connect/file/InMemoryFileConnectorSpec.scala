package zio.connect.file

import com.google.common.jimfs.{Configuration, Jimfs}
import zio.nio.connect.WatchServiceLayers
import zio.test.Live
import zio.{Scope, ZIO, ZLayer}

object InMemoryFileConnectorSpec extends FileConnectorSpec {

  override def spec =
    suite("InMemoryFileConnectorSpec")(fileConnectorSpec)
      .provideSome[Scope with Live](
        ZLayer.succeed(Jimfs.newFileSystem(Configuration.forCurrentPlatform())),
        zioFileSystem,
        FileOps.inMemory,
        WatchServiceLayers.inMemory,
        LiveFileConnector.layer
      )

  val zioFileSystem = ZLayer.fromZIO(
    for {
      fs <- ZIO.service[java.nio.file.FileSystem]
      r   = zio.nio.file.FileSystem.fromJava(fs)
    } yield r
  )

}
