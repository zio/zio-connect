package zio.connect.sqs

object TestSqsConnectorSpec extends SqsConnectorSpec {
  override def spec =
    suite("TestSqsConnectorSpec")(sqsConnectorSpec).provide(sqsConnectorTestLayer)
}
