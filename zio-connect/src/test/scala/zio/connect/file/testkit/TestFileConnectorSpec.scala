package zio.connect.file.testkit

import zio.Scope
import zio.connect.file.FileConnectorSpec

object TestFileConnectorSpec extends FileConnectorSpec {

  override def spec =
    suite("TestFileConnectorSpec")(fileConnectorSpec)
      .provideSome[Scope](TestFileConnector.layer)

}
