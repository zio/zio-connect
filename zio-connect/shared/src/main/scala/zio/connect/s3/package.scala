package zio.connect

import zio._
import zio.stream._
import zio.s3._
import software.amazon.awssdk.services.s3.model.S3Exception

package object s3 {
  type S3Connector

  def s3Layer: ZLayer[S3Credentials, ConnectionError, S3Connector] = ???

  def getObject(bucketName: String, key: String): ZStream[S3Connector, S3Exception, Byte] = ???

  // def putObject(bucketName: String, key: String): ZSink[S3Connector, S3Exception, Byte, Unit] = 
    ???

  // def deleteObjects: ZSink[S3Connector, S3Exception, ObjectId, Unit] = 
    ???

    // def move(locator: ObjectId => ObjectId): ZSink[S3Connector, S3Exception, ObjectId, Unit] = 
    ???
}
