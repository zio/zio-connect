package zio.connect.s3

import software.amazon.awssdk.regions.Region
import zio.aws.core.AwsError
import zio.aws.s3.model.primitives._
import zio.connect.s3.S3Connector.{CopyObject, MoveObject}
import zio.stream.{ZSink, ZStream}
import zio.{Trace, ZIO}

trait S3Connector {
  def copyObject(region: => Region)(implicit
    trace: Trace
  ): ZSink[Any, AwsError, CopyObject, CopyObject, Unit]

  final def copyObject(
    sourceRegion: => Region,
    destinationRegion: => Region
  )(implicit
    trace: Trace
  ): ZSink[Any, AwsError, CopyObject, CopyObject, Unit] =
    ZSink
      .foreach[Any, AwsError, CopyObject] { m =>
        getObject(m.sourceBucketName, m.objectKey, sourceRegion) >>> putObject(
          m.targetBucketName,
          m.objectKey,
          destinationRegion
        )
      }

  def createBucket(
    region: => Region
  )(implicit trace: Trace): ZSink[Any, AwsError, BucketName, BucketName, Unit]

  def deleteEmptyBucket(
    region: => Region
  )(implicit trace: Trace): ZSink[Any, AwsError, BucketName, BucketName, Unit]

  def deleteObjects(bucketName: => BucketName, region: => Region)(implicit
    trace: Trace
  ): ZSink[Any, AwsError, ObjectKey, ObjectKey, Unit]

  final def existsBucket(
    region: => Region
  )(implicit trace: Trace): ZSink[Any, AwsError, BucketName, BucketName, Boolean] =
    ZSink
      .take[BucketName](1)
      .map(_.headOption)
      .mapZIO {
        case Some(name) =>
          for {
            listResponse <- listBuckets(region).runCollect
            bucketExists  = listResponse.contains(name)
          } yield bucketExists
        case None => ZIO.succeed(false)
      }

  final def existsObject(
    bucket: => BucketName,
    region: => Region
  )(implicit trace: Trace): ZSink[Any, AwsError, ObjectKey, ObjectKey, Boolean] =
    ZSink
      .take[ObjectKey](1)
      .map(_.headOption)
      .mapZIO {
        case Some(key) =>
          for {
            listResponse <- listObjects(bucket, region).runCollect
            objectExists  = listResponse.contains(key)
          } yield objectExists
        case None => ZIO.succeed(false)
      }

  def getObject(bucketName: => BucketName, key: => ObjectKey, region: => Region)(implicit
    trace: Trace
  ): ZStream[Any, AwsError, Byte]

  def listBuckets(region: => Region)(implicit trace: Trace): ZStream[Any, AwsError, BucketName]

  def listObjects(bucketName: => BucketName, region: => Region)(implicit
    trace: Trace
  ): ZStream[Any, AwsError, ObjectKey]

  final def moveObject(region: => Region)(implicit
    trace: Trace
  ): ZSink[Any, AwsError, MoveObject, MoveObject, Unit] =
    moveObject(region, region)

  final def moveObject(sourceRegion: => Region, destinationRegion: => Region)(implicit
    trace: Trace
  ): ZSink[Any, AwsError, MoveObject, MoveObject, Unit] =
    ZSink
      .foreach[Any, AwsError, S3Connector.MoveObject] { m =>
        for {
          _ <- getObject(m.bucketName, m.objectKey, sourceRegion) >>> putObject(
                 m.targetBucketName,
                 m.targetObjectKey,
                 destinationRegion
               )
          _ <- ZStream(m.objectKey) >>> deleteObjects(m.bucketName, sourceRegion)
        } yield ()
      }

  def putObject(bucketName: => BucketName, key: => ObjectKey, region: => Region)(implicit
    trace: Trace
  ): ZSink[Any, AwsError, Byte, Nothing, Unit]

}

object S3Connector {

  final case class CopyObject(
    sourceBucketName: BucketName,
    objectKey: ObjectKey,
    targetBucketName: BucketName
  )

  final case class MoveObject(
    bucketName: BucketName,
    objectKey: ObjectKey,
    targetBucketName: BucketName,
    targetObjectKey: ObjectKey
  )

}
