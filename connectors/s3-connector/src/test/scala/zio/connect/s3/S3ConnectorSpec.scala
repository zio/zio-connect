package zio.connect.s3

import software.amazon.awssdk.services.s3.model.NoSuchBucketException
import zio.{Chunk, Random, ZIO}
import zio.connect.s3.S3Connector.{CopyObject, S3Exception}
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._

import java.util.UUID

trait S3ConnectorSpec extends ZIOSpecDefault {

  val s3ConnectorSpec =
    copyObjectSpec + createBucketSuite + deleteBucketSuite + listObjectsSuite + moveObjectSuite + putObjectSuite

  private lazy val copyObjectSpec =
    suite("copyObject")(
      test("replaces preexisting object") {
        val bucket1 = UUID.randomUUID().toString
        val bucket2 = UUID.randomUUID().toString
        val key     = UUID.randomUUID().toString
        for {
          _             <- ZStream(bucket1, bucket2) >>> createBucket
          content1      <- Random.nextBytes(5)
          content2      <- Random.nextBytes(5)
          _             <- ZStream.fromChunk(content1) >>> putObject(bucket1, key)
          _             <- ZStream.fromChunk(content2) >>> putObject(bucket2, key)
          _             <- ZStream(S3Connector.CopyObject(bucket1, key, bucket2)) >>> copyObject
          copiedContent <- getObject(bucket2, key).runCollect
        } yield assert(copiedContent)(equalTo(content1))
      },
      test("succeeds") {
        val bucket1 = UUID.randomUUID().toString
        val object1 = UUID.randomUUID().toString
        val object2 = UUID.randomUUID().toString
        val bucket2 = UUID.randomUUID().toString
        for {
          _             <- ZStream(bucket1, bucket2) >>> createBucket
          o1Content     <- Random.nextBytes(5)
          o2Content     <- Random.nextBytes(5)
          _             <- ZStream.fromChunk(o1Content) >>> putObject(bucket1, object1)
          _             <- ZStream.fromChunk(o2Content) >>> putObject(bucket1, object2)
          _             <- ZStream(CopyObject(bucket1, object1, bucket2), CopyObject(bucket1, object2, bucket2)) >>> copyObject
          initialFiles  <- listObjects(bucket2).runCollect
          movedFiles    <- listObjects(bucket2).runCollect
          movedContent1 <- getObject(bucket2, object1).runCollect
          movedContent2 <- getObject(bucket2, object2).runCollect

          filesWereMoved =
            Chunk(object1, object2).sorted == initialFiles.sorted && initialFiles.sorted == movedFiles.sorted
          o1CopyMatchesOriginal = o1Content == movedContent1
          o2CopyMatchesOriginal = o2Content == movedContent2
        } yield assertTrue(filesWereMoved) && assertTrue(o1CopyMatchesOriginal) && assertTrue(o2CopyMatchesOriginal)
      }
    )

  private lazy val createBucketSuite: Spec[S3Connector, S3Exception] =
    suite("createBucket")(
      test("changes nothing if bucket already exists") {
        val bucketName = UUID.randomUUID().toString
        for {
          _             <- ZStream(bucketName) >>> createBucket
          content       <- Random.nextBytes(5)
          _             <- ZStream.fromChunk(content) >>> putObject(bucketName, UUID.randomUUID().toString)
          objectsBefore <- listObjects(bucketName).runCollect
          _             <- ZStream(bucketName) >>> createBucket
          objectsAfter  <- listObjects(bucketName).runCollect
        } yield assert(objectsBefore)(equalTo(objectsAfter))
      },
      test("succeeds") {
        val bucketName = UUID.randomUUID().toString
        for {
          _          <- ZStream.succeed(bucketName) >>> createBucket
          wasCreated <- ZStream(bucketName) >>> existsBucket
          _          <- ZStream.succeed(bucketName) >>> deleteEmptyBuckets
          wasDeleted <- (ZStream(bucketName) >>> existsBucket).map(!_)
        } yield assertTrue(wasCreated) && assertTrue(wasDeleted)
      }
    )

  private lazy val deleteBucketSuite: Spec[S3Connector, S3Exception] =
    suite("deleteBucket")(
      test("fails if bucket is not empty") {
        val bucketName = UUID.randomUUID().toString
        for {
          _          <- ZStream.succeed(bucketName) >>> createBucket
          wasCreated <- ZStream(bucketName) >>> existsBucket
          _          <- ZStream.fromChunk[Byte](Chunk(1, 2, 3)) >>> putObject(bucketName, UUID.randomUUID().toString)
          wasDeleted <- (ZStream.succeed(bucketName) >>> deleteEmptyBuckets).as(true).catchSome { case _: S3Exception =>
                          ZIO.succeed(false)
                        }
        } yield assertTrue(wasCreated) && assert(wasDeleted)(equalTo(false))
      },
      test("succeeds if bucket is empty") {
        val bucketName = UUID.randomUUID().toString
        for {
          _          <- ZStream.succeed(bucketName) >>> createBucket
          wasCreated <- ZStream(bucketName) >>> existsBucket
          wasDeleted <- (ZStream.succeed(bucketName) >>> deleteEmptyBuckets).as(true)
        } yield assertTrue(wasCreated) && assertTrue(wasDeleted)
      }
    )

  private lazy val listObjectsSuite =
    suite("listObjects")(
      test("fails when bucket doesn't exist") {
        val bucketName = UUID.randomUUID().toString
        for {
          exit <- listObjects(bucketName).runCollect.exit
          failsWithExpectedError <-
            exit.as(false).catchSome { case S3Exception(err) =>
              ZIO.succeed(err.isInstanceOf[NoSuchBucketException])
            }
        } yield assertTrue(failsWithExpectedError)
      },
      test("succeeds") {
        val bucketName = UUID.randomUUID().toString
        val obj1       = UUID.randomUUID().toString
        val obj2       = UUID.randomUUID().toString
        for {
          _                   <- ZStream.succeed(bucketName) >>> createBucket
          testData            <- Random.nextBytes(5)
          _                   <- ZStream.fromChunk(testData) >>> putObject(bucketName, obj1)
          _                   <- ZStream.fromChunk(testData) >>> putObject(bucketName, obj2)
          actual              <- listObjects(bucketName).runCollect
          _                   <- ZStream.fromChunk(Chunk(obj1, obj2)) >>> deleteObjects(bucketName)
          afterObjectDeletion <- listObjects(bucketName).runCollect
        } yield assert(actual.sorted)(equalTo(Chunk(obj1, obj2).sorted)) && assertTrue(afterObjectDeletion.isEmpty)
      }
    )

  private lazy val moveObjectSuite =
    suite("moveObject")(
      test("replaces preexisting object") {
        val bucket1 = UUID.randomUUID().toString
        val bucket2 = UUID.randomUUID().toString
        val key     = UUID.randomUUID().toString
        for {
          _             <- ZStream(bucket1, bucket2) >>> createBucket
          content1      <- Random.nextBytes(5)
          content2      <- Random.nextBytes(5)
          _             <- ZStream.fromChunk(content1) >>> putObject(bucket1, key)
          _             <- ZStream.fromChunk(content2) >>> putObject(bucket2, key)
          _             <- ZStream(S3Connector.MoveObject(bucket1, key, bucket2, key)) >>> moveObject
          copiedContent <- getObject(bucket2, key).runCollect
        } yield assert(copiedContent)(equalTo(content1))
      },
      test("succeeds") {

        val bucket1 = UUID.randomUUID().toString
        val bucket2 = UUID.randomUUID().toString
        val key1    = UUID.randomUUID().toString
        val key2    = UUID.randomUUID().toString
        val key3    = UUID.randomUUID().toString
        val key4    = UUID.randomUUID().toString

        for {
          _                <- ZStream(bucket1, bucket2) >>> createBucket
          testDataK1       <- Random.nextBytes(5)
          _                <- ZStream.fromChunk(testDataK1) >>> putObject(bucket1, key1)
          testDataK2       <- Random.nextBytes(5)
          _                <- ZStream.fromChunk(testDataK2) >>> putObject(bucket1, key2)
          testDataK3       <- Random.nextBytes(5)
          _                <- ZStream.fromChunk(testDataK3) >>> putObject(bucket1, key3)
          initialB1Objects <- listObjects(bucket1).runCollect
          initialB2Objects <- listObjects(bucket2).runCollect
          _                <- ZStream(S3Connector.CopyObject(bucket1, key1, bucket2)) >>> copyObject
          _                <- ZStream(S3Connector.MoveObject(bucket1, key2, bucket2, key2)) >>> moveObject
          _                <- ZStream(S3Connector.MoveObject(bucket1, key3, bucket2, key4)) >>> moveObject
          b1Objects        <- listObjects(bucket1).runCollect
          b2Objects        <- listObjects(bucket2).runCollect

        } yield assert(initialB1Objects.sorted)(equalTo(Chunk(key1, key2, key3).sorted)) &&
          assertTrue(initialB2Objects.isEmpty) &&
          assert(b1Objects)(equalTo(Chunk(key1))) &&
          assert(b2Objects.sorted)(equalTo(Chunk(key1, key2, key4).sorted))

      }
    )

  private lazy val putObjectSuite: Spec[S3Connector, S3Exception] =
    suite("putObject")(
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
    )

}
