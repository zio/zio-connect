package zio.connect.file.testkit

import com.google.common.jimfs.{Configuration, Jimfs}
import zio.connect.file.FileConnectorSpec
import zio.{Scope, ZLayer}

object TestFileConnectorSpec extends FileConnectorSpec {

  override def spec =
    suite("TestFileConnectorSpec")(fileConnectorSpec)
      .provideSome[Scope](
        ZLayer.succeed(Jimfs.newFileSystem(Configuration.forCurrentPlatform())),
        TestFileOps.inMemory,
        TestFileConnector.layerWithCustomFileSystem
      )

}
