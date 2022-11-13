package zio.connect.s3.multiregion

object TestMultiRegionS3ConnectorSpec extends MultiRegionS3ConnectorSpec {

  override def spec =
    suite("TestMultiRegionS3ConnectorSpec")(s3ConnectorSpec).provide(multiRegionS3ConnectorTestLayer)

}
