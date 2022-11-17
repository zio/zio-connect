package zio.connect.kinesisdatastreams

object TestKinesisDataStreamsConnectorSpec extends KinesisDataStreamsConnectorSpec {

  override def spec =
    suite("TestKinesisDataStreamsConnectorSpec")(kinesisDataStreamsConnectorSpec).provide(
      kinesisDataStreamsConnectorTestLayer[String]
    )

}
