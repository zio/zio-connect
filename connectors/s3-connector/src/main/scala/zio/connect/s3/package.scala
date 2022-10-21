package zio.connect

import zio.Trace
import zio.connect.s3.S3Connector.S3Exception
import zio.stream.{ZSink, ZStream}

package object s3 {

  def createBucket(implicit trace: Trace): ZSink[S3Connector, S3Exception, String, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.createBucket)

  def deleteEmptyBuckets(implicit trace: Trace): ZSink[S3Connector, S3Exception, String, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.deleteEmptyBucket)

  def deleteObjects(bucketName: String)(implicit trace: Trace): ZSink[S3Connector, S3Exception, String, String, Unit] =
    ZSink.environmentWithSink(_.get.deleteObjects(bucketName))

  def existsBucket(implicit trace: Trace): ZSink[S3Connector, S3Exception, String, String, Boolean] =
    ZSink.environmentWithSink(_.get.existsBucket)

  def getObject(bucketName: => String, key: String)(implicit trace: Trace): ZStream[S3Connector, S3Exception, Byte] =
    ZStream.environmentWithStream(_.get.getObject(bucketName, key))

  def listObjects(bucketName: => String)(implicit trace: Trace): ZStream[S3Connector, S3Exception, String] =
    ZStream.environmentWithStream(_.get.listObjects(bucketName))

  val s3ConnectorLiveLayer = LiveS3Connector.live
//  val s3ConnectorTestLayer = TestS3Connector.live

  def putObject(bucketName: => String, key: String)(implicit
    trace: Trace
  ): ZSink[S3Connector, S3Exception, Byte, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.putObject(bucketName, key))

}
