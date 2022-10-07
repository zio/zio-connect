package zio.connect.file

import zio.Scope

object LiveFileConnectorSpec extends FileConnectorSpec {

  override def spec =
    suite("LiveFileConnectorSpec")(fileConnectorSpec)
      .provideSome[Scope](zio.connect.file.live)

}
