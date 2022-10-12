package zio.connect.s3

import zio.Trace
import zio.aws.core.AwsError
import zio.connect.s3.S3Connector.S3Exception
import zio.stream.{ZSink, ZStream}

//todo - update the S3Exception - turn it from an alias to a zio-connect specific type
trait S3Connector {

  def createBucket(implicit trace: Trace): ZSink[Any, S3Exception, String, Nothing, Unit]

  //todo: if non empty buckets can be deleted then rename this to deleteBucket
  def deleteEmptyBucket(implicit trace: Trace): ZSink[Any, S3Exception, String, Nothing, Unit]

  def deleteObject(bucketName: => String, keys: Iterable[String])(implicit
    trace: Trace
  ): ZSink[Any, S3Exception, S3Connector.ObjectId, Nothing, Unit]

  def existsBucket(name: => String)(implicit trace: Trace): ZStream[Any, S3Exception, Boolean]

  def getObject(bucketName: => String, key: String)(implicit trace: Trace): ZStream[Any, S3Exception, Byte]

  def listObjects(bucketName: => String)(implicit trace: Trace): ZStream[Any, S3Exception, S3Connector.ObjectId]

  def putObject(bucketName: => String, key: String)(implicit trace: Trace): ZSink[Any, S3Exception, Byte, Nothing, Unit]

}

object S3Connector {

  case class ObjectId(bucketName: String, objectKey: String)

  type S3Exception = AwsError

}
