package zio.connect.s3

object TestS3ConnectorSpec extends S3ConnectorSpec {

  override def spec =
    suite("TestS3ConnectorSpec")(s3ConnectorSpec).provide(s3ConnectorTestLayer)

}
