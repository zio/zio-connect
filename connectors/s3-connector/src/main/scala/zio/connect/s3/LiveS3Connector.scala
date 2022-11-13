package zio.connect.s3
import zio.aws.core.AwsError
import zio.aws.s3.S3
import zio.aws.s3.model.primitives.{BucketName => AwsBucketName, ContentLength, CopySource, ObjectKey => AwsObjectKey}
import zio.aws.s3.model.{
  CopyObjectRequest,
  CreateBucketRequest,
  Delete,
  DeleteBucketRequest,
  DeleteObjectsRequest,
  GetObjectRequest,
  ListObjectsRequest,
  ObjectIdentifier,
  PutObjectRequest
}
import zio.connect.s3.S3Connector.{BucketName, CopyObject, ObjectKey, Region, S3Exception}
import zio.stream.{ZSink, ZStream}
import zio.{Chunk, Trace, ZIO, ZLayer}

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

final case class LiveS3Connector(s3Map: Map[Region, S3]) extends S3Connector {
  private def getS3(region: Region) = ZIO
    .fromOption(s3Map.get(region))
    .orElseFail(S3Exception(new RuntimeException(s"Connector not found for Region $region")))

  override def copyObject(region: => Region)(implicit
    trace: Trace
  ): ZSink[Any, S3Exception, CopyObject, CopyObject, Unit] =
    ZSink
      .foreach[Any, S3Exception, CopyObject] { m =>
        getS3(region).flatMap { s3 =>
          s3.copyObject(
            CopyObjectRequest(
              destinationBucket = AwsBucketName(m.targetBucketName),
              destinationKey = AwsObjectKey(m.objectKey),
              copySource =
                CopySource(URLEncoder.encode(s"${m.sourceBucketName}/${m.objectKey}", StandardCharsets.UTF_8.toString))
            )
          ).mapError(a => S3Exception(a.toThrowable))
        }
      }

  override def createBucket(
    region: => Region
  )(implicit trace: Trace): ZSink[Any, S3Exception, BucketName, BucketName, Unit] =
    ZSink
      .foreach[Any, S3Exception, BucketName] { name =>
        getS3(region).flatMap(
          _.createBucket(CreateBucketRequest(bucket = AwsBucketName(name.toString))).mapError(a =>
            S3Exception(a.toThrowable)
          )
        )
      }

  override def deleteEmptyBucket(
    region: => Region
  )(implicit trace: Trace): ZSink[Any, S3Exception, BucketName, BucketName, Unit] =
    ZSink
      .foreach[Any, S3Exception, BucketName] { name =>
        getS3(region).flatMap(
          _.deleteBucket(DeleteBucketRequest(bucket = AwsBucketName(name))).mapError(a => S3Exception(a.toThrowable))
        )
      }

  override def deleteObjects(bucketName: => BucketName, region: => Region)(implicit
    trace: Trace
  ): ZSink[Any, S3Exception, ObjectKey, ObjectKey, Unit] =
    ZSink
      .foreachChunk[Any, S3Exception, ObjectKey] { objectKeys =>
        getS3(region).flatMap(
          _.deleteObjects(
            DeleteObjectsRequest(
              bucket = AwsBucketName(bucketName),
              delete = Delete(objects = objectKeys.map(a => ObjectIdentifier(AwsObjectKey(a))))
            )
          ).mapError(a => S3Exception(a.toThrowable))
        )
      }

  override def getObject(bucketName: => BucketName, key: => ObjectKey, region: => Region)(implicit
    trace: Trace
  ): ZStream[Any, S3Exception, Byte] =
    ZStream.unwrap(getS3(region).flatMap { s3 =>
      s3.getObject(GetObjectRequest(bucket = AwsBucketName(bucketName), key = AwsObjectKey(key)))
        .mapBoth(a => S3Exception(a.toThrowable), _.output)
        .map((r: ZStream[Any, AwsError, Byte]) => r.mapError(a => S3Exception(a.toThrowable)))
    })

  override def listBuckets(region: => Region)(implicit
    trace: Trace
  ): ZStream[Any, S3Exception, BucketName] =
    ZStream.fromIterableZIO {
      for {
        s3          <- getS3(region)
        listBuckets <- s3.listBuckets().mapError(a => S3Exception(a.toThrowable))
        buckets     <- listBuckets.getBuckets.mapError(a => S3Exception(a.toThrowable))
        bucketNames <- ZIO.succeed(buckets.flatMap(b => b.name.toList.map(BucketName(_))))

      } yield bucketNames
    }

  override def listObjects(bucketName: => BucketName, region: => Region)(implicit
    trace: Trace
  ): ZStream[Any, S3Exception, ObjectKey] =
    ZStream.fromIterableZIO {
      for {
        s3 <- getS3(region)
        objects <- s3.listObjects(ListObjectsRequest(bucket = AwsBucketName(bucketName)))
                     .mapBoth(
                       a => S3Exception(a.toThrowable),
                       _.contents.map(_.flatMap(_.key.toChunk.map(ObjectKey(_)))).getOrElse(Chunk.empty[ObjectKey])
                     )
      } yield objects

    }

  override def putObject(bucketName: => BucketName, key: => ObjectKey, region: => Region)(implicit
    trace: Trace
  ): ZSink[Any, S3Exception, Byte, Nothing, Unit] =
    ZSink
      .foreachChunk[Any, S3Exception, Byte] { content =>
        for {
          s3 <- getS3(region)
          _ <- s3.putObject(
                 request = PutObjectRequest(
                   bucket = AwsBucketName(bucketName),
                   key = AwsObjectKey(key),
                   contentLength = Some(ContentLength(content.length.toLong))
                 ),
                 body = ZStream.fromChunk(content).rechunk(1024)
               ).mapError(a => S3Exception(a.toThrowable))
        } yield ()
      }

}

object LiveS3Connector {

  val layer: ZLayer[Map[Region, S3], Nothing, LiveS3Connector] =
    ZLayer.fromZIO(ZIO.service[Map[Region, S3]].map(LiveS3Connector(_)))

}
