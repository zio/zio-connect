package zio.connect

import zio._
import zio.stream._

package object s3 {
  // Powered by ZIO S3
  type S3Connector
  type S3Credentials 
  type S3Exception
  type ConnectionError

  def s3Layer: ZLayer[S3Credentials, ConnectionError, S3Connector] = ???

  def getObject(bucketName: String, key: String): ZStream[S3Connector, S3Exception, Byte] = ???

  def putObject(bucketName: String, key: String): ZSink[S3Connector, S3Exception, Byte, Unit] = 
    ???

  def deleteObjects: ZSink[S3Connector, S3Exception, ObjectId, Unit] = 
    ???

    def move(locator: ObjectId => ObjectId): ZSink[S3Connector, S3Exception, ObjectId, Unit] = 
    ???
}
