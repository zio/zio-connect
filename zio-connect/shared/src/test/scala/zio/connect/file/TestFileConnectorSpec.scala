package zio.connect.file

import zio.Scope
import zio.connect.file.testkit.TestFileConnector

object TestFileConnectorSpec extends FileConnectorSpec {

  override def spec =
    suite("TestKitFileConnectorSpec")(fileConnectorSpec)
      .provideSome[Scope](TestFileConnector.layer)

}
