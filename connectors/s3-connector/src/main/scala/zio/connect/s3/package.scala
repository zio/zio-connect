package zio.connect

import software.amazon.awssdk.regions.Region
import zio.aws.core.AwsError
import zio.aws.s3.S3
import zio.aws.s3.model.primitives.{BucketName, ObjectKey}
import zio.connect.s3.S3Connector.CopyObject
import zio.stream.{ZSink, ZStream}
import zio.{Trace, ZLayer}

package object s3 {

  def copyObject(region: => Region)(implicit
    trace: Trace
  ): ZSink[S3Connector, AwsError, CopyObject, CopyObject, Unit] =
    ZSink.serviceWithSink(_.copyObject(region))

  def copyObject(sourceRegion: => Region, destinationRegion: => Region)(implicit
    trace: Trace
  ): ZSink[S3Connector, AwsError, CopyObject, CopyObject, Unit] =
    ZSink.serviceWithSink(_.copyObject(sourceRegion, destinationRegion))

  def createBucket(region: => Region)(implicit
    trace: Trace
  ): ZSink[S3Connector, AwsError, BucketName, BucketName, Unit] =
    ZSink.serviceWithSink(_.createBucket(region))

  def deleteEmptyBucket(region: => Region)(implicit
    trace: Trace
  ): ZSink[S3Connector, AwsError, BucketName, BucketName, Unit] =
    ZSink.serviceWithSink(_.deleteEmptyBucket(region))

  def deleteObjects(bucketName: => BucketName, region: => Region)(implicit
    trace: Trace
  ): ZSink[S3Connector, AwsError, ObjectKey, ObjectKey, Unit] =
    ZSink.serviceWithSink(_.deleteObjects(bucketName, region))

  def existsBucket(region: => Region)(implicit
    trace: Trace
  ): ZSink[S3Connector, AwsError, BucketName, BucketName, Boolean] =
    ZSink.serviceWithSink(_.existsBucket(region))

  def existsObject(bucketName: => BucketName, region: => Region)(implicit
    trace: Trace
  ): ZSink[S3Connector, AwsError, ObjectKey, ObjectKey, Boolean] =
    ZSink.serviceWithSink(_.existsObject(bucketName, region))

  def getObject(bucketName: => BucketName, key: => ObjectKey, region: => Region)(implicit
    trace: Trace
  ): ZStream[S3Connector, AwsError, Byte] =
    ZStream.serviceWithStream(_.getObject(bucketName, key, region))

  def listBuckets(region: => Region)(implicit
    trace: Trace
  ): ZStream[S3Connector, AwsError, BucketName] =
    ZStream.serviceWithStream(_.listBuckets(region))

  def listObjects(bucketName: => BucketName, region: => Region)(implicit
    trace: Trace
  ): ZStream[S3Connector, AwsError, ObjectKey] =
    ZStream.serviceWithStream(_.listObjects(bucketName, region))

  def moveObject(region: => Region)(implicit
    trace: Trace
  ): ZSink[S3Connector, AwsError, S3Connector.MoveObject, S3Connector.MoveObject, Unit] =
    ZSink.serviceWithSink(_.moveObject(region))

  def moveObject(sourceRegion: => Region, destinationRegion: => Region)(implicit
    trace: Trace
  ): ZSink[S3Connector, AwsError, S3Connector.MoveObject, S3Connector.MoveObject, Unit] =
    ZSink.serviceWithSink(_.moveObject(sourceRegion, destinationRegion))

  val s3ConnectorLiveLayer: ZLayer[Map[Region, S3], Nothing, LiveS3Connector] = LiveS3Connector.layer
  val s3ConnectorTestLayer: ZLayer[Any, Nothing, TestS3Connector]             = TestS3Connector.layer

  def putObject(bucketName: => BucketName, key: => ObjectKey, region: => Region)(implicit
    trace: Trace
  ): ZSink[S3Connector, AwsError, Byte, Nothing, Unit] =
    ZSink.serviceWithSink(_.putObject(bucketName, key, region))

}
