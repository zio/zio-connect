package zio.connect.s3

import zio.{Trace, ZIO}
import zio.connect.s3.S3Connector.{CopyObject, MoveObject, S3Exception}
import zio.stream.{ZSink, ZStream}

trait S3Connector {

  def copyObject(implicit trace: Trace): ZSink[Any, S3Exception, CopyObject, CopyObject, Unit]

  def createBucket(implicit trace: Trace): ZSink[Any, S3Exception, String, String, Unit]

  def deleteEmptyBucket(implicit trace: Trace): ZSink[Any, S3Exception, String, String, Unit]

  def deleteObjects(bucketName: => String)(implicit
    trace: Trace
  ): ZSink[Any, S3Exception, String, String, Unit]

  final def existsBucket(implicit trace: Trace): ZSink[Any, S3Exception, String, String, Boolean] =
    ZSink
      .take[String](1)
      .map(_.headOption)
      .mapZIO {
        case Some(name) =>
          for {
            listResponse <- listBuckets.runCollect
            bucketExists  = listResponse.contains(name)
          } yield bucketExists
        case None => ZIO.succeed(false)
      }

  final def existsObject(bucket: => String)(implicit trace: Trace): ZSink[Any, S3Exception, String, String, Boolean] =
    ZSink
      .take[String](1)
      .map(_.headOption)
      .mapZIO {
        case Some(key) =>
          for {
            listResponse <- listObjects(bucket).runCollect
            objectExists  = listResponse.contains(key)
          } yield objectExists
        case None => ZIO.succeed(false)
      }

  def getObject(bucketName: => String, key: String)(implicit trace: Trace): ZStream[Any, S3Exception, Byte]

  def listBuckets(implicit trace: Trace): ZStream[Any, S3Exception, String]

  def listObjects(bucketName: => String)(implicit trace: Trace): ZStream[Any, S3Exception, String]

  def moveObject(implicit trace: Trace): ZSink[Any, S3Exception, MoveObject, MoveObject, Unit]

  def putObject(bucketName: => String, key: String)(implicit trace: Trace): ZSink[Any, S3Exception, Byte, Nothing, Unit]

}

object S3Connector {

  case class S3Exception(reason: Throwable)

  final case class CopyObject(
    sourceBucketName: String,
    objectKey: String,
    targetBucketName: String
  )

  final case class MoveObject(
    bucketName: String,
    objectKey: String,
    targetBucketName: String,
    targetObjectKey: String
  )

}
