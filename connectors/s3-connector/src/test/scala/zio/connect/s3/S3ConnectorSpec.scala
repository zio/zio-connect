package zio.connect.s3

import zio.{Chunk, ZIO}
import zio.connect.s3.S3Connector.S3Exception
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._

import java.util.UUID

trait S3ConnectorSpec extends ZIOSpecDefault {

  val s3ConnectorSpec = createBucketSuite + deleteBucketSuite

  private lazy val createBucketSuite: Spec[S3Connector, S3Exception] =
    suite("createBucket") {
      test("succeeds") {
        val bucketName = UUID.randomUUID().toString
        for {
          _          <- ZStream.succeed(bucketName) >>> createBucket
          wasCreated <- existsBucket(bucketName).runHead.map(_.contains(true))
          _          <- ZStream.succeed(bucketName) >>> deleteEmptyBuckets
          wasDeleted <- existsBucket(bucketName).runHead.map(_.contains(false))
        } yield assertTrue(wasCreated) && assertTrue(wasDeleted)
      }
    }

  private lazy val deleteBucketSuite: Spec[S3Connector, S3Exception] =
    suite("deleteBucket") {
      test("fails if not empty") {
        val bucketName = UUID.randomUUID().toString
        for {
          _          <- ZStream.succeed(bucketName) >>> createBucket
          wasCreated <- existsBucket(bucketName).runHead.map(_.contains(true))
          _          <- ZStream.fromChunk[Byte](Chunk(1, 2, 3)) >>> putObject(bucketName, UUID.randomUUID().toString)
          wasDeleted <- (ZStream.succeed(bucketName) >>> deleteEmptyBuckets).as(true).catchSome { case _: S3Exception =>
                          ZIO.succeed(false)
                        }
        } yield assertTrue(wasCreated) && assert(wasDeleted)(equalTo(false))
      }
      test("succeeds when bucket is empty") {
        val bucketName = UUID.randomUUID().toString
        for {
          _          <- ZStream.succeed(bucketName) >>> createBucket
          wasCreated <- existsBucket(bucketName).runHead.map(_.contains(true))
          wasDeleted <- (ZStream.succeed(bucketName) >>> deleteEmptyBuckets).as(true).catchSome { case _: S3Exception =>
                          ZIO.succeed(false)
                        }
        } yield assertTrue(wasCreated) && assertTrue(wasDeleted)
      }
    }

}
