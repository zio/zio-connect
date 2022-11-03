package zio.connect.s3
import zio.aws.core.AwsError
import zio.aws.s3.S3
import zio.aws.s3.model.primitives.{BucketName => AwsBucketName, ContentLength, CopySource, ObjectKey => AwsObjectKey}
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
import zio.connect.s3.S3Connector.{BucketName, CopyObject, ObjectKey, S3Exception}
import zio.stream.{ZSink, ZStream}
import zio.{Chunk, Trace, ZIO, ZLayer}

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

final case class LiveS3Connector(s3: S3) extends S3Connector {

  override def copyObject(implicit trace: Trace): ZSink[Any, S3Exception, CopyObject, CopyObject, Unit] =
    ZSink
      .foreach[Any, AwsError, CopyObject] { m =>
        s3.copyObject(
          CopyObjectRequest(
            destinationBucket = AwsBucketName(m.targetBucketName),
            destinationKey = AwsObjectKey(m.objectKey),
            copySource =
              CopySource(URLEncoder.encode(s"${m.sourceBucketName}/${m.objectKey}", StandardCharsets.UTF_8.toString))
          )
        )
      }
      .mapError(a => S3Exception(a.toThrowable))

  override def createBucket(implicit trace: Trace): ZSink[Any, S3Exception, BucketName, BucketName, Unit] =
    ZSink
      .foreach[Any, AwsError, BucketName] { name =>
        s3.createBucket(CreateBucketRequest(bucket = AwsBucketName(name)))
      }
      .mapError(a => S3Exception(a.toThrowable))

  override def deleteEmptyBucket(implicit trace: Trace): ZSink[Any, S3Exception, BucketName, BucketName, Unit] =
    ZSink
      .foreach[Any, AwsError, BucketName] { name =>
        s3.deleteBucket(DeleteBucketRequest(bucket = AwsBucketName(name)))
      }
      .mapError(a => S3Exception(a.toThrowable))

  override def deleteObjects(bucketName: => BucketName)(implicit
    trace: Trace
  ): ZSink[Any, S3Exception, ObjectKey, ObjectKey, Unit] =
    ZSink
      .foreachChunk[Any, AwsError, ObjectKey] { objectKeys =>
        s3.deleteObjects(
          DeleteObjectsRequest(
            bucket = AwsBucketName(bucketName),
            delete = Delete(objects = objectKeys.map(a => ObjectIdentifier(AwsObjectKey(a))))
          )
        )
      }
      .mapError(a => S3Exception(a.toThrowable))

  override def getObject(bucketName: => BucketName, key: => ObjectKey)(implicit
    trace: Trace
  ): ZStream[Any, S3Exception, Byte] =
    ZStream
      .unwrap(
        s3.getObject(GetObjectRequest(bucket = AwsBucketName(bucketName), key = AwsObjectKey(key)))
          .map(a => a.output)
      )
      .mapError(a => S3Exception(a.toThrowable))

  override def listBuckets(implicit
    trace: Trace
  ): ZStream[Any, S3Exception, BucketName] =
    ZStream
      .fromIterableZIO(
        s3.listBuckets().flatMap(_.getBuckets.map(_.flatMap(_.name.toList.map(BucketName(_)))))
      )
      .mapError(a => S3Exception(a.toThrowable))

  override def listObjects(bucketName: => BucketName)(implicit
    trace: Trace
  ): ZStream[Any, S3Exception, ObjectKey] =
    ZStream
      .fromIterableZIO(
        s3.listObjects(ListObjectsRequest(bucket = AwsBucketName(bucketName)))
          .map(_.contents.map(_.flatMap(_.key.toChunk.map(ObjectKey(_)))).getOrElse(Chunk.empty[ObjectKey]))
      )
      .mapError(a => S3Exception(a.toThrowable))

  override def moveObject(implicit
    trace: Trace
  ): ZSink[Any, S3Exception, S3Connector.MoveObject, Nothing, Unit] =
    ZSink
      .foreach[Any, AwsError, S3Connector.MoveObject] { m =>
        s3.copyObject(
          CopyObjectRequest(
            destinationBucket = AwsBucketName(m.targetBucketName),
            destinationKey = AwsObjectKey(m.targetObjectKey),
            copySource = CopySource(URLEncoder.encode(s"${m.bucketName}/${m.objectKey}", StandardCharsets.UTF_8))
          )
        ) *> s3.deleteObject(
          DeleteObjectRequest(
            bucket = AwsBucketName(m.bucketName),
            key = AwsObjectKey(m.objectKey)
          )
        )
      }
      .mapError(a => S3Exception(a.toThrowable))

  override def putObject(bucketName: => BucketName, key: => ObjectKey)(implicit
    trace: Trace
  ): ZSink[Any, S3Exception, Byte, Nothing, Unit] =
    ZSink
      .foreachChunk[Any, AwsError, Byte] { content =>
        s3.putObject(
          request = PutObjectRequest(
            bucket = AwsBucketName(bucketName),
            key = AwsObjectKey(key),
            contentLength = Some(ContentLength(content.length.toLong))
          ),
          body = ZStream.fromChunk(content).rechunk(1024)
        )
      }
      .mapError(a => S3Exception(a.toThrowable))
}

object LiveS3Connector {

  val layer: ZLayer[S3, Nothing, LiveS3Connector] =
    ZLayer.fromZIO(ZIO.service[S3].map(LiveS3Connector(_)))

}
