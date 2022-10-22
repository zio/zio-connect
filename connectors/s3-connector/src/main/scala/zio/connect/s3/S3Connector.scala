package zio.connect.s3

import zio.Trace
import zio.connect.s3.S3Connector.{CopyObject, MoveObject, S3Exception}
import zio.stream.{ZSink, ZStream}

trait S3Connector {

  def copyObject(implicit trace: Trace): ZSink[Any, S3Exception, CopyObject, CopyObject, Unit]

  def createBucket(implicit trace: Trace): ZSink[Any, S3Exception, String, String, Unit]

  def deleteEmptyBucket(implicit trace: Trace): ZSink[Any, S3Exception, String, String, Unit]

  def deleteObjects(bucketName: => String)(implicit
    trace: Trace
  ): ZSink[Any, S3Exception, String, String, Unit]

  def existsBucket(implicit trace: Trace): ZSink[Any, S3Exception, String, String, Boolean]

  def getObject(bucketName: => String, key: String)(implicit trace: Trace): ZStream[Any, S3Exception, Byte]

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
