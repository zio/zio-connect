package zio.connect.fs2

object LiveFS2ConnectorSpec extends FS2ConnectorSpec {

  override def spec =
    suite("LiveFS2ConnectorSpec")(fs2ConnectorSpec)
      .provide(fs2ConnectorLiveLayer)

}
