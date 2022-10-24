package zio.connect

import zio.Trace
import zio.connect.s3.S3Connector.{BucketName, CopyObject, ObjectKey, S3Exception}
import zio.stream.{ZSink, ZStream}

package object s3 {

  def copyObject(implicit trace: Trace): ZSink[S3Connector, S3Exception, CopyObject, CopyObject, Unit] =
    ZSink.serviceWithSink(_.copyObject)

  def createBucket(implicit trace: Trace): ZSink[S3Connector, S3Exception, BucketName, BucketName, Unit] =
    ZSink.serviceWithSink(_.createBucket)

  def deleteEmptyBuckets(implicit trace: Trace): ZSink[S3Connector, S3Exception, BucketName, BucketName, Unit] =
    ZSink.serviceWithSink(_.deleteEmptyBucket)

  def deleteObjects(bucketName: BucketName)(implicit
    trace: Trace
  ): ZSink[S3Connector, S3Exception, ObjectKey, ObjectKey, Unit] =
    ZSink.serviceWithSink(_.deleteObjects(bucketName))

  def existsBucket(implicit trace: Trace): ZSink[S3Connector, S3Exception, BucketName, BucketName, Boolean] =
    ZSink.serviceWithSink(_.existsBucket)

  def existsObject(bucketName: => BucketName)(implicit
    trace: Trace
  ): ZSink[S3Connector, S3Exception, ObjectKey, ObjectKey, Boolean] =
    ZSink.serviceWithSink(_.existsObject(bucketName))

  def getObject(bucketName: => BucketName, key: ObjectKey)(implicit
    trace: Trace
  ): ZStream[S3Connector, S3Exception, Byte] =
    ZStream.serviceWithStream(_.getObject(bucketName, key))

  def listObjects(bucketName: => BucketName)(implicit trace: Trace): ZStream[S3Connector, S3Exception, ObjectKey] =
    ZStream.serviceWithStream(_.listObjects(bucketName))

  def moveObject(implicit
    trace: Trace
  ): ZSink[S3Connector, S3Exception, S3Connector.MoveObject, S3Connector.MoveObject, Unit] =
    ZSink.serviceWithSink(_.moveObject)

  val s3ConnectorLiveLayer = LiveS3Connector.layer
  val s3ConnectorTestLayer = TestS3Connector.layer

  def putObject(bucketName: => BucketName, key: ObjectKey)(implicit
    trace: Trace
  ): ZSink[S3Connector, S3Exception, Byte, Nothing, Unit] =
    ZSink.serviceWithSink(_.putObject(bucketName, key))

}
