package zio.connect.s3.singleregion

import zio.aws.core.AwsError
import zio.aws.s3.S3
import zio.aws.s3.model._
import zio.aws.s3.model.primitives._
import zio.connect.s3.S3Connector
import zio.connect.s3.S3Connector.{CopyObject, MoveObject}
import zio.stream.{ZSink, ZStream}
import zio.{Chunk, Trace, ZIO, ZLayer}

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

final case class SingleRegionLiveS3Connector(s3: S3) extends SingleRegionS3Connector {

  override def copyObject(implicit
    trace: Trace
  ): ZSink[Any, AwsError, CopyObject, CopyObject, Unit] =
    ZSink
      .foreach[Any, AwsError, CopyObject] { m =>
        for {
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

  override def createBucket(implicit trace: Trace): ZSink[Any, AwsError, BucketName, BucketName, Unit] =
    ZSink
      .foreach[Any, AwsError, BucketName] { name =>
        s3.createBucket(CreateBucketRequest(bucket = BucketName(name)))

      }

  override def deleteEmptyBucket(implicit trace: Trace): ZSink[Any, AwsError, BucketName, BucketName, Unit] =
    ZSink
      .foreach[Any, AwsError, BucketName] { name =>
        s3.deleteBucket(DeleteBucketRequest(bucket = BucketName(name)))
      }

  override def deleteObjects(bucketName: => BucketName)(implicit
    trace: Trace
  ): ZSink[Any, AwsError, ObjectKey, ObjectKey, Unit] =
    ZSink
      .foreachChunk[Any, AwsError, ObjectKey] { objectKeys =>
        s3.deleteObjects(
          DeleteObjectsRequest(
            bucket = BucketName(bucketName),
            delete = Delete(objects = objectKeys.map(a => ObjectIdentifier(ObjectKey(a))))
          )
        )
      }

  override def getObject(bucketName: => BucketName, key: => ObjectKey)(implicit
    trace: Trace
  ): ZStream[Any, AwsError, Byte] =
    ZStream.unwrap(
      s3.getObject(GetObjectRequest(bucket = BucketName(bucketName), key = ObjectKey(key)))
        .map(_.output)
        .map((r: ZStream[Any, AwsError, Byte]) => r)
    )

  override def listBuckets(implicit
    trace: Trace
  ): ZStream[Any, AwsError, BucketName] =
    ZStream.fromIterableZIO {
      for {
        listBuckets <- s3.listBuckets()
        buckets     <- listBuckets.getBuckets
        bucketNames <- ZIO.succeed(buckets.flatMap(b => b.name.toList.map(BucketName(_))))

      } yield bucketNames
    }

  override def listObjects(bucketName: => BucketName)(implicit
    trace: Trace
  ): ZStream[Any, AwsError, ObjectKey] =
    ZStream.fromIterableZIO {
      for {
        objects <- s3.listObjects(ListObjectsRequest(bucket = BucketName(bucketName)))
                     .map(
                       _.contents.map(_.flatMap(_.key.toChunk.map(ObjectKey(_)))).getOrElse(Chunk.empty[ObjectKey])
                     )
      } yield objects

    }

  override def moveObject(implicit trace: Trace): ZSink[Any, AwsError, MoveObject, MoveObject, Unit] =
    ZSink
      .foreach[Any, AwsError, S3Connector.MoveObject] { m =>
        for {
          _ <- getObject(m.bucketName, m.objectKey) >>> putObject(
                 m.targetBucketName,
                 m.targetObjectKey
               )
          _ <- ZStream(m.objectKey) >>> deleteObjects(m.bucketName)
        } yield ()
      }

  override def putObject(bucketName: => BucketName, key: => ObjectKey)(implicit
    trace: Trace
  ): ZSink[Any, AwsError, Byte, Nothing, Unit] =
    ZSink
      .foreachChunk[Any, AwsError, Byte] { content =>
        for {
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

object SingleRegionLiveS3Connector {

  val layer: ZLayer[S3, Nothing, SingleRegionLiveS3Connector] =
    ZLayer.fromZIO(ZIO.service[S3].map(SingleRegionLiveS3Connector(_)))

}
