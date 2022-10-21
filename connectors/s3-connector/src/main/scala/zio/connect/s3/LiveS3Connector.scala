package zio.connect.s3
import zio.aws.core.AwsError
import zio.{Chunk, Trace, ZIO, ZLayer}
import zio.aws.s3.S3
import zio.aws.s3.model.{
  CreateBucketRequest,
  Delete,
  DeleteBucketRequest,
  DeleteObjectsRequest,
  GetObjectRequest,
  ListObjectsRequest,
  ObjectIdentifier,
  PutObjectRequest
}
import zio.aws.s3.model.primitives.{BucketName, ContentLength, ObjectKey}
import zio.connect.s3.S3Connector.S3Exception
import zio.stream.{ZSink, ZStream}

case class LiveS3Connector(s3: S3) extends S3Connector {

  override def createBucket(implicit trace: Trace): ZSink[Any, S3Exception, String, Nothing, Unit] =
    ZSink.foreach { name: String =>
      s3.createBucket(CreateBucketRequest(bucket = BucketName(name)))
    }.mapError(a => S3Exception(a.toThrowable))

  override def deleteEmptyBucket(implicit trace: Trace): ZSink[Any, S3Exception, String, Nothing, Unit] =
    ZSink.foreach { name: String =>
      s3.deleteBucket(DeleteBucketRequest(bucket = BucketName(name)))
    }.mapError(a => S3Exception(a.toThrowable))

  override def deleteObjects(bucketName: => String)(implicit
    trace: Trace
  ): ZSink[Any, S3Exception, String, String, Unit] =
    ZSink
      .foreachChunk[Any, AwsError, String] { objectKeys =>
        s3.deleteObjects(
          DeleteObjectsRequest(
            bucket = BucketName(bucketName),
            delete = Delete(objects = objectKeys.map(a => ObjectIdentifier(ObjectKey(a))))
          )
        )
      }
      .mapError(a => S3Exception(a.toThrowable))

  override def existsBucket(implicit trace: Trace): ZSink[Any, S3Exception, String, String, Boolean] =
    ZSink
      .take[String](1)
      .map(_.headOption)
      .mapZIO {
        case Some(name) =>
          for {
            listResponse <- s3.listBuckets().flatMap(_.getBuckets)
            bucketExists  = listResponse.exists(_.name.contains(name))
          } yield bucketExists
        case None => ZIO.succeed(false)
      }
      .mapError(a => S3Exception(a.toThrowable))

  override def getObject(bucketName: => String, key: String)(implicit trace: Trace): ZStream[Any, S3Exception, Byte] =
    ZStream
      .unwrap(
        s3.getObject(GetObjectRequest(bucket = BucketName(bucketName), key = ObjectKey(key)))
          .map(a => a.output)
      )
      .mapError(a => S3Exception(a.toThrowable))

  override def listObjects(bucketName: => String)(implicit
    trace: Trace
  ): ZStream[Any, S3Exception, String] =
    ZStream
      .fromIterableZIO(
        s3.listObjects(ListObjectsRequest(bucket = BucketName(bucketName)))
          .map(_.contents.map(_.flatMap(_.key.toChunk)).getOrElse(Chunk.empty[String]))
      )
      .mapError(a => S3Exception(a.toThrowable))

  override def putObject(bucketName: => String, key: String)(implicit
    trace: Trace
  ): ZSink[Any, S3Exception, Byte, Nothing, Unit] =
    ZSink.foreachChunk { content: Chunk[Byte] =>
      s3.putObject(
        request = PutObjectRequest(
          bucket = BucketName(bucketName),
          key = ObjectKey(key),
          contentLength = Some(ContentLength(content.length.toLong))
        ),
        body = ZStream.fromChunk(content).rechunk(1024)
      )
    }.mapError(a => S3Exception(a.toThrowable))
}

object LiveS3Connector {

  val live: ZLayer[S3, Throwable, LiveS3Connector] =
    ZLayer.fromZIO(ZIO.service[S3].map(LiveS3Connector(_)))

}
