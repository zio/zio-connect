package zio.connect.s3.singleregion


object TestSingleRegionS3ConnectorSpec extends SingleRegionS3ConnectorSpec {

  override def spec =
    suite("TestSingleRegionS3ConnectorSpec")(s3ConnectorSpec).provide(singleRegionS3ConnectorTestLayer)

}
