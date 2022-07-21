package zio.connect.s3

import software.amazon.awssdk.services.s3.model.S3Exception
import zio.stream.ZStream

trait S3Connector {

  def getObject(bucketName: String, key: String): ZStream[Any, S3Exception, Byte]

}

object S3Connector {

  def getObject(bucketName: String, key: String): ZStream[S3Connector, S3Exception, Byte] =
    ZStream.environmentWithStream(_.get.getObject(bucketName, key))

  // def putObject(bucketName: String, key: String): ZSink[S3Connector, S3Exception, Byte, Unit] =
  ???

  // def deleteObjects: ZSink[S3Connector, S3Exception, ObjectId, Unit] =
  ???

  // def move(locator: ObjectId => ObjectId): ZSink[S3Connector, S3Exception, ObjectId, Unit] =
  ???
}
