package zio.connect.s3
import zio.aws.core.AwsError
import zio.{Chunk, Trace, ZIO, ZLayer}
import zio.aws.s3.S3
import zio.aws.s3.model.{
  CopyObjectRequest,
  CreateBucketRequest,
  Delete,
  DeleteBucketRequest,
  DeleteObjectRequest,
  DeleteObjectsRequest,
  GetObjectRequest,
  ListObjectsRequest,
  ObjectIdentifier,
  PutObjectRequest
}
import zio.aws.s3.model.primitives.{BucketName, ContentLength, CopySource, ObjectKey}
import zio.connect.s3.S3Connector.{CopyObject, S3Exception}
import zio.stream.{ZSink, ZStream}

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

case class LiveS3Connector(s3: S3) extends S3Connector {

  override def copyObject(implicit trace: Trace): ZSink[Any, S3Exception, CopyObject, CopyObject, Unit] =
    ZSink
      .foreach[Any, AwsError, CopyObject] { m =>
        s3.copyObject(
          CopyObjectRequest(
            destinationBucket = BucketName(m.targetBucketName),
            destinationKey = ObjectKey(m.objectKey),
            copySource =
              CopySource(URLEncoder.encode(s"${m.sourceBucketName}/${m.objectKey}", StandardCharsets.UTF_8.toString))
          )
        )
      }
      .mapError(a => S3Exception(a.toThrowable))

  override def createBucket(implicit trace: Trace): ZSink[Any, S3Exception, String, String, Unit] =
    ZSink.foreach { name: String =>
      s3.createBucket(CreateBucketRequest(bucket = BucketName(name)))
    }.mapError(a => S3Exception(a.toThrowable))

  override def deleteEmptyBucket(implicit trace: Trace): ZSink[Any, S3Exception, String, String, Unit] =
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

  override def getObject(bucketName: => String, key: String)(implicit trace: Trace): ZStream[Any, S3Exception, Byte] =
    ZStream
      .unwrap(
        s3.getObject(GetObjectRequest(bucket = BucketName(bucketName), key = ObjectKey(key)))
          .map(a => a.output)
      )
      .mapError(a => S3Exception(a.toThrowable))

  override def listBuckets(implicit
    trace: Trace
  ): ZStream[Any, S3Exception, String] =
    ZStream
      .fromIterableZIO(
        s3.listBuckets().flatMap(_.getBuckets.map(_.flatMap(_.name.toList)))
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

  override def moveObject(implicit
    trace: Trace
  ): ZSink[Any, S3Exception, S3Connector.MoveObject, Nothing, Unit] =
    ZSink
      .foreach[Any, AwsError, S3Connector.MoveObject] { m =>
        s3.copyObject(
          CopyObjectRequest(
            destinationBucket = BucketName(m.targetBucketName),
            destinationKey = ObjectKey(m.targetObjectKey),
            copySource =
              CopySource(URLEncoder.encode(s"${m.bucketName}/${m.objectKey}", StandardCharsets.UTF_8.toString))
          )
        ) *> s3.deleteObject(
          DeleteObjectRequest(
            bucket = BucketName(m.bucketName),
            key = ObjectKey(m.objectKey)
          )
        )
      }
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

  val layer: ZLayer[S3, Nothing, LiveS3Connector] =
    ZLayer.fromZIO(ZIO.service[S3].map(LiveS3Connector(_)))

}
