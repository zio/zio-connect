package zio.connect

import zio.Trace
import zio.connect.s3.S3Connector.S3Exception
import zio.stream.{ZSink, ZStream}

package object s3 {

  def createBucket(implicit trace: Trace): ZSink[S3Connector, S3Exception, String, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.createBucket)

  def deleteEmptyBuckets(implicit trace: Trace): ZSink[S3Connector, S3Exception, String, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.deleteEmptyBucket)

  def existsBucket(name: => String)(implicit trace: Trace): ZStream[S3Connector, S3Exception, Boolean] =
    ZStream.environmentWithStream(_.get.existsBucket(name))

  def listObjects(bucketName: => String)(implicit trace: Trace): ZStream[S3Connector, S3Exception, String] =
    ZStream.environmentWithStream(_.get.listObjects(bucketName))

  val live = LiveS3Connector.live

  def putObject(bucketName: => String, key: String)(implicit
    trace: Trace
  ): ZSink[S3Connector, S3Exception, Byte, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.putObject(bucketName, key))

}
