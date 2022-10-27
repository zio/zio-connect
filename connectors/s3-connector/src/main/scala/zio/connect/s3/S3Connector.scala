package zio.connect.s3

import zio.connect.s3.S3Connector.{BucketName, CopyObject, MoveObject, ObjectKey, S3Exception}
import zio.prelude.Newtype
import zio.stream.{ZSink, ZStream}
import zio.{Trace, ZIO}

trait S3Connector {

  def copyObject(implicit trace: Trace): ZSink[Any, S3Exception, CopyObject, CopyObject, Unit]

  def createBucket(implicit trace: Trace): ZSink[Any, S3Exception, BucketName, BucketName, Unit]

  def deleteEmptyBucket(implicit trace: Trace): ZSink[Any, S3Exception, BucketName, BucketName, Unit]

  def deleteObjects(bucketName: => BucketName)(implicit
    trace: Trace
  ): ZSink[Any, S3Exception, ObjectKey, ObjectKey, Unit]

  final def existsBucket(implicit trace: Trace): ZSink[Any, S3Exception, BucketName, BucketName, Boolean] =
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
  )(implicit trace: Trace): ZSink[Any, S3Exception, ObjectKey, ObjectKey, Boolean] =
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

  def getObject(bucketName: => BucketName, key: => ObjectKey)(implicit trace: Trace): ZStream[Any, S3Exception, Byte]

  def listBuckets(implicit trace: Trace): ZStream[Any, S3Exception, BucketName]

  def listObjects(bucketName: => BucketName)(implicit trace: Trace): ZStream[Any, S3Exception, ObjectKey]

  def moveObject(implicit trace: Trace): ZSink[Any, S3Exception, MoveObject, MoveObject, Unit]

  def putObject(bucketName: => BucketName, key: => ObjectKey)(implicit
    trace: Trace
  ): ZSink[Any, S3Exception, Byte, Nothing, Unit]

}

object S3Connector {

  object BucketName extends Newtype[String]
  type BucketName = BucketName.Type

  object ObjectKey extends Newtype[String]
  type ObjectKey = ObjectKey.Type

  case class S3Exception(reason: Throwable)

  final case class CopyObject(
    sourceBucketName: BucketName,
    objectKey: ObjectKey,
    targetBucketName: BucketName
  )

  final case class MoveObject(
    bucketName: BucketName,
    objectKey: ObjectKey,
    targetBucketName: BucketName,
    targetObjectKey: ObjectKey
  )

}
