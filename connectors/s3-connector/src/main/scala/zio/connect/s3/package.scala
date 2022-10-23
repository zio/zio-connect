package zio.connect

import zio.Trace
import zio.connect.s3.S3Connector.{CopyObject, S3Exception}
import zio.stream.{ZSink, ZStream}

package object s3 {

  def copyObject(implicit trace: Trace): ZSink[S3Connector, S3Exception, CopyObject, CopyObject, Unit] =
    ZSink.environmentWithSink(_.get.copyObject)

  def createBucket(implicit trace: Trace): ZSink[S3Connector, S3Exception, String, String, Unit] =
    ZSink.environmentWithSink(_.get.createBucket)

  def deleteEmptyBuckets(implicit trace: Trace): ZSink[S3Connector, S3Exception, String, String, Unit] =
    ZSink.environmentWithSink(_.get.deleteEmptyBucket)

  def deleteObjects(bucketName: String)(implicit trace: Trace): ZSink[S3Connector, S3Exception, String, String, Unit] =
    ZSink.environmentWithSink(_.get.deleteObjects(bucketName))

  def existsBucket(implicit trace: Trace): ZSink[S3Connector, S3Exception, String, String, Boolean] =
    ZSink.environmentWithSink(_.get.existsBucket)

  def existsObject(bucketName: => String)(implicit
    trace: Trace
  ): ZSink[S3Connector, S3Exception, String, String, Boolean] =
    ZSink.environmentWithSink(_.get.existsObject(bucketName))

  def getObject(bucketName: => String, key: String)(implicit trace: Trace): ZStream[S3Connector, S3Exception, Byte] =
    ZStream.environmentWithStream(_.get.getObject(bucketName, key))

  def listObjects(bucketName: => String)(implicit trace: Trace): ZStream[S3Connector, S3Exception, String] =
    ZStream.environmentWithStream(_.get.listObjects(bucketName))

  def moveObject(implicit
    trace: Trace
  ): ZSink[S3Connector, S3Exception, S3Connector.MoveObject, S3Connector.MoveObject, Unit] =
    ZSink.environmentWithSink[S3Connector](_.get.moveObject)

  val s3ConnectorLiveLayer = LiveS3Connector.layer
  val s3ConnectorTestLayer = TestS3Connector.layer

  def putObject(bucketName: => String, key: String)(implicit
    trace: Trace
  ): ZSink[S3Connector, S3Exception, Byte, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.putObject(bucketName, key))

}
