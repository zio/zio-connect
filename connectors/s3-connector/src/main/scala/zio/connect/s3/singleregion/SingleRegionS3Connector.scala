package zio.connect.s3.singleregion

import zio.aws.core.AwsError
import zio.aws.s3.model.primitives._
import zio.connect.s3.S3Connector.{CopyObject, MoveObject}
import zio.stream.{ZSink, ZStream}
import zio.{Trace, ZIO}

trait SingleRegionS3Connector {

  def copyObject(implicit
    trace: Trace
  ): ZSink[Any, AwsError, CopyObject, CopyObject, Unit]

  def createBucket(implicit trace: Trace): ZSink[Any, AwsError, BucketName, BucketName, Unit]

  def deleteEmptyBucket(implicit trace: Trace): ZSink[Any, AwsError, BucketName, BucketName, Unit]

  def deleteObjects(bucketName: => BucketName)(implicit
    trace: Trace
  ): ZSink[Any, AwsError, ObjectKey, ObjectKey, Unit]

  final def existsBucket(implicit trace: Trace): ZSink[Any, AwsError, BucketName, BucketName, Boolean] =
    ZSink
      .take[BucketName](1)
      .map(_.headOption)
      .mapZIO {
        case Some(name) =>
          for {
            listResponse <- listBuckets.runCollect
            bucketExists  = listResponse.contains(name)
          } yield bucketExists
        case None => ZIO.succeed(false)
      }

  final def existsObject(
    bucket: => BucketName
  )(implicit trace: Trace): ZSink[Any, AwsError, ObjectKey, ObjectKey, Boolean] =
    ZSink
      .take[ObjectKey](1)
      .map(_.headOption)
      .mapZIO {
        case Some(key) =>
          for {
            listResponse <- listObjects(bucket).runCollect
            objectExists  = listResponse.contains(key)
          } yield objectExists
        case None => ZIO.succeed(false)
      }

  def getObject(bucketName: => BucketName, key: => ObjectKey)(implicit
    trace: Trace
  ): ZStream[Any, AwsError, Byte]

  def listBuckets(implicit trace: Trace): ZStream[Any, AwsError, BucketName]

  def listObjects(bucketName: => BucketName)(implicit
    trace: Trace
  ): ZStream[Any, AwsError, ObjectKey]

  def moveObject(implicit
    trace: Trace
  ): ZSink[Any, AwsError, MoveObject, MoveObject, Unit]

  def putObject(bucketName: => BucketName, key: => ObjectKey)(implicit
    trace: Trace
  ): ZSink[Any, AwsError, Byte, Nothing, Unit]

}
