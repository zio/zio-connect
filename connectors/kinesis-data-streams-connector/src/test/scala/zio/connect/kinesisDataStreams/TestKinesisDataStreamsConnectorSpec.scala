package zio.connect.kinesisDataStreams

object TestKinesisDataStreamsConnectorSpec extends KinesisDataStreamsConnectorSpec {

  override def spec =
    suite("TestKinesisDataStreamsConnectorSpec")(kinesisDataStreamsConnectorSpec).provide(
      TestKinesisDataStreamsConnector.layer[String]
    )

}
