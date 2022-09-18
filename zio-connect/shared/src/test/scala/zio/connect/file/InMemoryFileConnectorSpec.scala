package zio.connect.file

import zio.Scope
import zio.connect.file.testkit.InMemoryFileConnector

object InMemoryFileConnectorSpec extends FileConnectorSpec {

  override def spec =
    suite("TestKitFileConnectorSpec")(fileConnectorSpec)
      .provideSome[Scope](InMemoryFileConnector.layer)

}
