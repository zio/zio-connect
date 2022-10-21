package zio.connect.s3

import zio.Trace
import zio.connect.s3.S3Connector.S3Exception
import zio.stream.{ZSink, ZStream}

trait S3Connector {

  def createBucket(implicit trace: Trace): ZSink[Any, S3Exception, String, Nothing, Unit]

  def deleteEmptyBucket(implicit trace: Trace): ZSink[Any, S3Exception, String, Nothing, Unit]

  def deleteObject(bucketName: => String, keys: Iterable[String])(implicit
    trace: Trace
  ): ZSink[Any, S3Exception, S3Connector.ObjectId, Nothing, Unit]

  def existsBucket(implicit trace: Trace): ZSink[Any, S3Exception, String, String, Boolean]

  def getObject(bucketName: => String, key: String)(implicit trace: Trace): ZStream[Any, S3Exception, Byte]

  def listObjects(bucketName: => String)(implicit trace: Trace): ZStream[Any, S3Exception, String]

  def putObject(bucketName: => String, key: String)(implicit trace: Trace): ZSink[Any, S3Exception, Byte, Nothing, Unit]

}

object S3Connector {

  case class ObjectId(bucketName: String, objectKey: String)

  case class S3Exception(reason: Throwable)

}
