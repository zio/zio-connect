package zio.connect

import zio.aws.s3.S3
import zio.connect.s3.S3Connector.{BucketName, CopyObject, ObjectKey, Region, S3Exception}
import zio.stream.{ZSink, ZStream}
import zio.{Trace, ZLayer}

package object s3 {
  val usEast1Region = "us-east-1"
  val usWest2Region = "us-west-2"

  def copyObject(region: => Region = usEast1Region)(implicit
    trace: Trace
  ): ZSink[S3Connector, S3Exception, CopyObject, CopyObject, Unit] =
    ZSink.serviceWithSink(_.copyObject(region))

  def copyObject(sourceRegion: => Region, destinationRegion: => Region)(implicit
    trace: Trace
  ): ZSink[S3Connector, S3Exception, CopyObject, CopyObject, Unit] =
    ZSink.serviceWithSink(_.copyObject(sourceRegion, destinationRegion))

  def createBucket(region: => Region = usEast1Region)(implicit
    trace: Trace
  ): ZSink[S3Connector, S3Exception, BucketName, BucketName, Unit] =
    ZSink.serviceWithSink(_.createBucket(region))

  def deleteEmptyBucket(region: => Region = usEast1Region)(implicit
    trace: Trace
  ): ZSink[S3Connector, S3Exception, BucketName, BucketName, Unit] =
    ZSink.serviceWithSink(_.deleteEmptyBucket(region))

  def deleteObjects(bucketName: BucketName, region: => Region)(implicit
    trace: Trace
  ): ZSink[S3Connector, S3Exception, ObjectKey, ObjectKey, Unit] =
    ZSink.serviceWithSink(_.deleteObjects(bucketName, region))

  def existsBucket(region: => Region = usEast1Region)(implicit
    trace: Trace
  ): ZSink[S3Connector, S3Exception, BucketName, BucketName, Boolean] =
    ZSink.serviceWithSink(_.existsBucket(region))

  def existsObject(bucketName: => BucketName, region: => Region)(implicit
    trace: Trace
  ): ZSink[S3Connector, S3Exception, ObjectKey, ObjectKey, Boolean] =
    ZSink.serviceWithSink(_.existsObject(bucketName, region))

  def getObject(bucketName: => BucketName, key: ObjectKey, region: => Region)(implicit
    trace: Trace
  ): ZStream[S3Connector, S3Exception, Byte] =
    ZStream.serviceWithStream(_.getObject(bucketName, key, region))

  def listBuckets(region: => Region = usEast1Region)(implicit
    trace: Trace
  ): ZStream[S3Connector, S3Exception, BucketName] =
    ZStream.serviceWithStream(_.listBuckets(region))

  def listObjects(bucketName: => BucketName, region: => Region)(implicit
    trace: Trace
  ): ZStream[S3Connector, S3Exception, ObjectKey] =
    ZStream.serviceWithStream(_.listObjects(bucketName, region))

  def moveObject(region: => Region = usEast1Region)(implicit
    trace: Trace
  ): ZSink[S3Connector, S3Exception, S3Connector.MoveObject, S3Connector.MoveObject, Unit] =
    ZSink.serviceWithSink(_.moveObject(region))

  def moveObject(sourceRegion: => Region, destinationRegion: => Region)(implicit
    trace: Trace
  ): ZSink[S3Connector, S3Exception, S3Connector.MoveObject, S3Connector.MoveObject, Unit] =
    ZSink.serviceWithSink(_.moveObject(sourceRegion, destinationRegion))

  val s3ConnectorLiveLayer: ZLayer[Map[Region, S3], Nothing, LiveS3Connector] = LiveS3Connector.layer
  val s3ConnectorTestLayer: ZLayer[Any, Nothing, TestS3Connector]             = TestS3Connector.layer

  def putObject(bucketName: => BucketName, key: ObjectKey, region: => Region = usEast1Region)(implicit
    trace: Trace
  ): ZSink[S3Connector, S3Exception, Byte, Nothing, Unit] =
    ZSink.serviceWithSink(_.putObject(bucketName, key, region))

}
