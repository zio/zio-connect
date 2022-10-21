package zio.connect.s3

import zio.{Chunk, Random, ZIO}
import zio.connect.s3.S3Connector.S3Exception
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._

import java.util.UUID

trait S3ConnectorSpec extends ZIOSpecDefault {

  val s3ConnectorSpec = createBucketSuite + deleteBucketSuite + listObjectsSuite + putObjectSuite

  private lazy val createBucketSuite: Spec[S3Connector, S3Exception] =
    suite("createBucket") {
      test("succeeds") {
        val bucketName = UUID.randomUUID().toString
        for {
          _          <- ZStream.succeed(bucketName) >>> createBucket
          wasCreated <- ZStream(bucketName) >>> existsBucket
          _          <- ZStream.succeed(bucketName) >>> deleteEmptyBuckets
          wasDeleted <- (ZStream(bucketName) >>> existsBucket).map(!_)
        } yield assertTrue(wasCreated) && assertTrue(wasDeleted)
      }
    }

  private lazy val deleteBucketSuite: Spec[S3Connector, S3Exception] =
    suite("deleteBucket") {
      test("fails if not empty") {
        val bucketName = UUID.randomUUID().toString
        for {
          _          <- ZStream.succeed(bucketName) >>> createBucket
          wasCreated <- ZStream(bucketName) >>> existsBucket
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
          wasCreated <- ZStream(bucketName) >>> existsBucket
          wasDeleted <- (ZStream.succeed(bucketName) >>> deleteEmptyBuckets).as(true).catchSome { case _: S3Exception =>
                          ZIO.succeed(false)
                        }
        } yield assertTrue(wasCreated) && assertTrue(wasDeleted)
      }
    }

  private lazy val listObjectsSuite =
    suite("listObjects") {
      test("succeeds") {
        val bucketName = UUID.randomUUID().toString
        val obj1       = UUID.randomUUID().toString
        val obj2       = UUID.randomUUID().toString
        for {
          _        <- ZStream.succeed(bucketName) >>> createBucket
          testData <- Random.nextBytes(5)
          _        <- ZStream.fromChunk(testData) >>> putObject(bucketName, obj1)
          _        <- ZStream.fromChunk(testData) >>> putObject(bucketName, obj2)
          actual   <- listObjects(bucketName).runCollect
        } yield assert(actual.sorted)(equalTo(Chunk(obj1, obj2).sorted))
      }
    }

  private lazy val putObjectSuite: Spec[S3Connector, S3Exception] =
    suite("putObject") {
      test("succeeds") {
        val bucketName = UUID.randomUUID().toString
        val objectKey  = UUID.randomUUID().toString
        for {
          _        <- ZStream.succeed(bucketName) >>> createBucket
          testData <- Random.nextBytes(5)
          _        <- ZStream.fromChunk(testData) >>> putObject(bucketName, objectKey)
          actual   <- getObject(bucketName, objectKey).runCollect
        } yield assert(actual)(equalTo(testData))
      }
    }

}
