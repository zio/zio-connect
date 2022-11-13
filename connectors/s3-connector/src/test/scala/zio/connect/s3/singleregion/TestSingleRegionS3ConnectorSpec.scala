package zio.connect.s3.singleregion

import zio.connect.s3.s3ConnectorTestLayer


object TestSingleRegionS3ConnectorSpec extends SingleRegionS3ConnectorSpec {

  override def spec =
    suite("TestSingleRegionS3ConnectorSpec")(s3ConnectorSpec).provide(s3ConnectorTestLayer)

}
