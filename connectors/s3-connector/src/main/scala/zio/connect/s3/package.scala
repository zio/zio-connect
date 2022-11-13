package zio.connect

import software.amazon.awssdk.regions.Region
import zio.ZLayer
import zio.aws.s3.S3
import zio.connect.s3.multiregion.{MultiRegionLiveS3Connector, MultiRegionS3Connector, TestMultiRegionS3Connector}
import zio.connect.s3.singleregion.{SingleRegionLiveS3Connector, SingleRegionS3Connector, TestSingleRegionS3Connector}

package object s3 {

  val s3ConnectorLiveLayer: ZLayer[S3, Nothing, SingleRegionS3Connector]  = SingleRegionLiveS3Connector.layer
  val s3ConnectorTestLayer: ZLayer[Any, Nothing, SingleRegionS3Connector] = TestSingleRegionS3Connector.layer

  val multiRegionS3ConnectorLiveLayer: ZLayer[Map[Region, S3], Nothing, MultiRegionS3Connector] =
    MultiRegionLiveS3Connector.layer
  val multiRegionS3ConnectorTestLayer: ZLayer[Any, Nothing, MultiRegionS3Connector] = TestMultiRegionS3Connector.layer

}
