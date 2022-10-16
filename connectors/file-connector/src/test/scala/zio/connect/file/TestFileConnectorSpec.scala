package zio.connect.file

import zio.Scope

object TestFileConnectorSpec extends FileConnectorSpec {

  override def spec =
    suite("TestFileConnectorSpec")(fileConnectorSpec)
      .provideSome[Scope](zio.connect.file.fileConnectorTestLayer)

}
