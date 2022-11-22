package zio.connect.s3.multiregion

import zio.connect.s3.multiRegionS3ConnectorTestLayer

object TestMultiRegionS3ConnectorSpec extends MultiRegionS3ConnectorSpec {

  override def spec =
    suite("TestMultiRegionS3ConnectorSpec")(s3ConnectorSpec).provide(multiRegionS3ConnectorTestLayer)

}
