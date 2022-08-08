package zio.connect.file

import zio.Scope

object LiveFileConnectorSpec extends FileConnectorSpec {

  override def spec =
    fileConnectorSpec.provideSome[Scope](LiveFileConnector.layer)

}
