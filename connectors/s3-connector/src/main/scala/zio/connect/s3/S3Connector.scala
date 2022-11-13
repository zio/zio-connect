package zio.connect.s3

import zio.connect.s3.S3Connector._
import zio.prelude.{Newtype, Subtype}
import zio.stream.{ZSink, ZStream}
import zio.{Trace, ZIO}

import scala.language.implicitConversions
trait S3Connector {
  final def copyObject(region: => Region)(implicit
    trace: Trace
  ): ZSink[Any, S3Exception, CopyObject, CopyObject, Unit] = copyObject(region, region)

  final def copyObject(
    sourceRegion: => Region,
    destinationRegion: => Region
  )(implicit
    trace: Trace
  ): ZSink[Any, S3Exception, CopyObject, CopyObject, Unit] =
    ZSink
      .foreach[Any, S3Exception, CopyObject] { m =>
        getObject(m.sourceBucketName, m.objectKey, sourceRegion) >>> putObject(
          m.targetBucketName,
          m.objectKey,
          destinationRegion
        )
      }

  def createBucket(
    region: => Region
  )(implicit trace: Trace): ZSink[Any, S3Exception, BucketName, BucketName, Unit]

  def deleteEmptyBucket(
    region: => Region
  )(implicit trace: Trace): ZSink[Any, S3Exception, BucketName, BucketName, Unit]

  def deleteObjects(bucketName: => BucketName, region: => Region)(implicit
    trace: Trace
  ): ZSink[Any, S3Exception, ObjectKey, ObjectKey, Unit]

  final def existsBucket(
    region: => Region
  )(implicit trace: Trace): ZSink[Any, S3Exception, BucketName, BucketName, Boolean] =
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
  )(implicit trace: Trace): ZSink[Any, S3Exception, ObjectKey, ObjectKey, Boolean] =
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
  ): ZStream[Any, S3Exception, Byte]

  def listBuckets(region: => Region)(implicit trace: Trace): ZStream[Any, S3Exception, BucketName]

  def listObjects(bucketName: => BucketName, region: => Region)(implicit
    trace: Trace
  ): ZStream[Any, S3Exception, ObjectKey]

  final def moveObject(region: => Region)(implicit
    trace: Trace
  ): ZSink[Any, S3Exception, MoveObject, MoveObject, Unit] =
    moveObject(region, region)

  final def moveObject(sourceRegion: => Region, destinationRegion: => Region)(implicit
    trace: Trace
  ): ZSink[Any, S3Exception, MoveObject, MoveObject, Unit] =
    ZSink
      .foreach[Any, S3Exception, S3Connector.MoveObject] { m =>
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
  ): ZSink[Any, S3Exception, Byte, Nothing, Unit]

}

object S3Connector {

  object BucketName extends Subtype[String]
  type BucketName = BucketName.Type

  object ObjectKey extends Subtype[String]
  type ObjectKey = ObjectKey.Type

  object Region extends Newtype[String]

  type Region = Region.Type
  implicit def stringToRegion(region: String): Region.Type = Region(region)
  case class S3Exception(reason: Throwable)

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
