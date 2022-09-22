package zio.connect.s3

import software.amazon.awssdk.services.s3.model.S3Exception
import zio.s3.{ConnectionError, S3Credentials}
import zio.stream.ZStream
import zio.{Trace, ZLayer}

case class LiveS3Connector(s3Credentials: S3Credentials) extends S3Connector {
  override def getObject(bucketName: => String, key: => String)(implicit
    trace: Trace
  ): ZStream[Any, S3Exception, Byte] = ???
}

object LiveS3Connector {
  val layer: ZLayer[S3Credentials, ConnectionError, S3Connector] = ???
}
