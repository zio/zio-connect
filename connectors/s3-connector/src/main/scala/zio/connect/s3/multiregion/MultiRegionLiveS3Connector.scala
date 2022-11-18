package zio.connect.s3.multiregion

import software.amazon.awssdk.regions.Region
import zio.aws.core.AwsError
import zio.aws.s3.S3
import zio.aws.s3.model._
import zio.aws.s3.model.primitives._
import zio.connect.s3.S3Connector.CopyObject
import zio.stream.{ZSink, ZStream}
import zio.{Chunk, Trace, URIO, ZIO, ZLayer}

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

final case class MultiRegionLiveS3Connector(s3Map: Map[Region, S3]) extends MultiRegionS3Connector {

  override def copyObject(region: => Region)(implicit
    trace: Trace
  ): ZSink[Any, AwsError, CopyObject, CopyObject, Unit] =
    ZSink
      .foreach[Any, AwsError, CopyObject] { m =>
        for {
          s3 <- getS3(region)
          copySource <-
            ZIO
              .attempt(URLEncoder.encode(s"${m.sourceBucketName}/${m.objectKey}", StandardCharsets.UTF_8.toString))
              .orDie
          _ <- s3.copyObject(
                 CopyObjectRequest(
                   destinationBucket = BucketName(m.targetBucketName),
                   destinationKey = ObjectKey(m.objectKey),
                   copySource = CopySource(copySource)
                 )
               )
        } yield ()
      }

  private def getS3(region: Region): URIO[Any, S3] = ZIO
    .fromOption(s3Map.get(region))
    .orElseFail(new RuntimeException(s"Connector not found for Region $region"))
    .orDie

  override def createBucket(
    region: => Region
  )(implicit trace: Trace): ZSink[Any, AwsError, BucketName, BucketName, Unit] =
    ZSink
      .foreach[Any, AwsError, BucketName] { name =>
        getS3(region).flatMap(
          _.createBucket(CreateBucketRequest(bucket = BucketName(name)))
        )
      }

  override def deleteEmptyBucket(
    region: => Region
  )(implicit trace: Trace): ZSink[Any, AwsError, BucketName, BucketName, Unit] =
    ZSink
      .foreach[Any, AwsError, BucketName] { name =>
        getS3(region).flatMap(
          _.deleteBucket(DeleteBucketRequest(bucket = BucketName(name)))
        )
      }

  override def deleteObjects(bucketName: => BucketName, region: => Region)(implicit
    trace: Trace
  ): ZSink[Any, AwsError, ObjectKey, ObjectKey, Unit] =
    ZSink
      .foreachChunk[Any, AwsError, ObjectKey] { objectKeys =>
        getS3(region).flatMap(
          _.deleteObjects(
            DeleteObjectsRequest(
              bucket = BucketName(bucketName),
              delete = Delete(objects = objectKeys.map(a => ObjectIdentifier(ObjectKey(a))))
            )
          )
        )
      }

  override def getObject(bucketName: => BucketName, key: => ObjectKey, region: => Region)(implicit
    trace: Trace
  ): ZStream[Any, AwsError, Byte] =
    ZStream.unwrap(getS3(region).flatMap { s3 =>
      s3.getObject(GetObjectRequest(bucket = BucketName(bucketName), key = ObjectKey(key)))
        .map(_.output)
        .map((r: ZStream[Any, AwsError, Byte]) => r)
    })

  override def listBuckets(region: => Region)(implicit
    trace: Trace
  ): ZStream[Any, AwsError, BucketName] =
    ZStream.fromIterableZIO {
      for {
        s3          <- getS3(region)
        listBuckets <- s3.listBuckets()
        buckets     <- listBuckets.getBuckets
        bucketNames <- ZIO.succeed(buckets.flatMap(b => b.name.toList.map(BucketName(_))))

      } yield bucketNames
    }

  override def listObjects(bucketName: => BucketName, region: => Region)(implicit
    trace: Trace
  ): ZStream[Any, AwsError, ObjectKey] =
    ZStream.fromIterableZIO {
      for {
        s3 <- getS3(region)
        objects <- s3.listObjects(ListObjectsRequest(bucket = BucketName(bucketName)))
                     .map(
                       _.contents.map(_.flatMap(_.key.toChunk.map(ObjectKey(_)))).getOrElse(Chunk.empty[ObjectKey])
                     )
      } yield objects

    }

  override def putObject(bucketName: => BucketName, key: => ObjectKey, region: => Region)(implicit
    trace: Trace
  ): ZSink[Any, AwsError, Byte, Nothing, Unit] =
    ZSink
      .foreachChunk[Any, AwsError, Byte] { content =>
        for {
          s3 <- getS3(region)
          _ <- s3.putObject(
                 request = PutObjectRequest(
                   bucket = BucketName(bucketName),
                   key = ObjectKey(key),
                   contentLength = Some(ContentLength(content.length.toLong))
                 ),
                 body = ZStream.fromChunk(content).rechunk(1024)
               )
        } yield ()
      }

}

object MultiRegionLiveS3Connector {

  val layer: ZLayer[Map[Region, S3], Nothing, MultiRegionS3Connector] =
    ZLayer.fromZIO(ZIO.service[Map[Region, S3]].map(MultiRegionLiveS3Connector(_)))

}
