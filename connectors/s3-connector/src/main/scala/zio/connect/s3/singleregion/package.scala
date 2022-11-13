package zio.connect.s3

import zio.{Trace, ZLayer}
import zio.aws.core.AwsError
import zio.aws.s3.S3
import zio.aws.s3.model.primitives.{BucketName, ObjectKey}
import zio.connect.s3.S3Connector.CopyObject
import zio.stream.{ZSink, ZStream}

package object singleregion {

  def copyObject(implicit
    trace: Trace
  ): ZSink[SingleRegionS3Connector, AwsError, CopyObject, CopyObject, Unit] =
    ZSink.serviceWithSink(_.copyObject)

  def createBucket(implicit
    trace: Trace
  ): ZSink[SingleRegionS3Connector, AwsError, BucketName, BucketName, Unit] =
    ZSink.serviceWithSink(_.createBucket)

  def deleteEmptyBucket(implicit
    trace: Trace
  ): ZSink[SingleRegionS3Connector, AwsError, BucketName, BucketName, Unit] =
    ZSink.serviceWithSink(_.deleteEmptyBucket)

  def deleteObjects(bucketName: => BucketName)(implicit
    trace: Trace
  ): ZSink[SingleRegionS3Connector, AwsError, ObjectKey, ObjectKey, Unit] =
    ZSink.serviceWithSink(_.deleteObjects(bucketName))

  def existsBucket(implicit
    trace: Trace
  ): ZSink[SingleRegionS3Connector, AwsError, BucketName, BucketName, Boolean] =
    ZSink.serviceWithSink(_.existsBucket)

  def existsObject(bucketName: => BucketName)(implicit
    trace: Trace
  ): ZSink[SingleRegionS3Connector, AwsError, ObjectKey, ObjectKey, Boolean] =
    ZSink.serviceWithSink(_.existsObject(bucketName))

  def getObject(bucketName: => BucketName, key: => ObjectKey)(implicit
    trace: Trace
  ): ZStream[SingleRegionS3Connector, AwsError, Byte] =
    ZStream.serviceWithStream(_.getObject(bucketName, key))

  def listBuckets(implicit
    trace: Trace
  ): ZStream[SingleRegionS3Connector, AwsError, BucketName] =
    ZStream.serviceWithStream(_.listBuckets)

  def listObjects(bucketName: => BucketName)(implicit
    trace: Trace
  ): ZStream[SingleRegionS3Connector, AwsError, ObjectKey] =
    ZStream.serviceWithStream(_.listObjects(bucketName))

  def moveObject(implicit
    trace: Trace
  ): ZSink[SingleRegionS3Connector, AwsError, S3Connector.MoveObject, S3Connector.MoveObject, Unit] =
    ZSink.serviceWithSink(_.moveObject)

  val singleRegionS3ConnectorLiveLayer: ZLayer[S3, Nothing, SingleRegionS3Connector] = SingleRegionLiveS3Connector.layer
  val singleRegionS3ConnectorTestLayer: ZLayer[Any, Nothing, SingleRegionS3Connector] =
    TestSingleRegionS3Connector.layer

  def putObject(bucketName: => BucketName, key: => ObjectKey)(implicit
    trace: Trace
  ): ZSink[SingleRegionS3Connector, AwsError, Byte, Nothing, Unit] =
    ZSink.serviceWithSink(_.putObject(bucketName, key))

}
