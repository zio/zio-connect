package zio.connect.s3.singleregion

import zio.aws.core.AwsError
import zio.aws.s3.model.primitives.{BucketName, ObjectKey}
import zio.connect.s3.S3Connector
import zio.connect.s3.S3Connector.CopyObject
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._
import zio.{Chunk, Random, ZIO}

import java.util.UUID

trait SingleRegionS3ConnectorSpec extends ZIOSpecDefault {

  val s3ConnectorSpec =
    copyObjectSpec + createBucketSuite + deleteBucketSuite + deleteObjectsSuite + listObjectsSuite + moveObjectSuite + putObjectSuite

  private lazy val copyObjectSpec =
    suite("copyObject")(
      test("replaces preexisting object") {
        val bucket1 = BucketName(UUID.randomUUID().toString)
        val bucket2 = BucketName(UUID.randomUUID().toString)
        val key     = ObjectKey(UUID.randomUUID().toString)
        for {
          _             <- ZStream(bucket1, bucket2) >>> createBucket
          content1      <- Random.nextBytes(5)
          content2      <- Random.nextBytes(5)
          _             <- ZStream.fromChunk(content1) >>> putObject(bucket1, key)
          _             <- ZStream.fromChunk(content2) >>> putObject(bucket2, key)
          _             <- ZStream(S3Connector.CopyObject(bucket1, key, bucket2)) >>> copyObject
          copiedContent <- getObject(bucket2, key).runCollect
        } yield assertTrue(copiedContent == content1)
      },
      test("succeeds") {
        val bucket1 = BucketName(UUID.randomUUID().toString)
        val object1 = ObjectKey(UUID.randomUUID().toString)
        val object2 = ObjectKey(UUID.randomUUID().toString)
        val bucket2 = BucketName(UUID.randomUUID().toString)
        for {
          _              <- ZStream(bucket1, bucket2) >>> createBucket
          o1Content      <- Random.nextBytes(5)
          o2Content      <- Random.nextBytes(5)
          _              <- ZStream.fromChunk(o1Content) >>> putObject(bucket1, object1)
          _              <- ZStream.fromChunk(o2Content) >>> putObject(bucket1, object2)
          initialFiles   <- listObjects(bucket1).runCollect
          _              <- ZStream(CopyObject(bucket1, object1, bucket2), CopyObject(bucket1, object2, bucket2)) >>> copyObject
          copiedFiles    <- listObjects(bucket2).runCollect
          copiedContent1 <- getObject(bucket2, object1).runCollect
          copiedContent2 <- getObject(bucket2, object2).runCollect

          filesWereCopied =
            Chunk(object1, object2).sortBy(_.toString) == initialFiles.sortBy(_.toString) &&
              initialFiles.sortBy(_.toString) == copiedFiles.sortBy(_.toString)
          o1CopyMatchesOriginal = o1Content == copiedContent1
          o2CopyMatchesOriginal = o2Content == copiedContent2
        } yield assertTrue(filesWereCopied) && assertTrue(o1CopyMatchesOriginal) && assertTrue(o2CopyMatchesOriginal)
      }
    )

  private lazy val createBucketSuite =
    suite("createBucket")(
      test("fails if bucket already exists") {
        val bucketName = BucketName(UUID.randomUUID().toString)
        for {
          _            <- ZStream(bucketName) >>> createBucket
          bucketExists <- ZStream(bucketName) >>> existsBucket
          exit         <- (ZStream(bucketName) >>> createBucket).exit
          failsWithExpectedError <-
            exit.as(false).catchSome { case _: AwsError => ZIO.succeed(true) }
        } yield assertTrue(bucketExists) && assertTrue(failsWithExpectedError)
      },
      test("succeeds") {
        val bucketName = BucketName(UUID.randomUUID().toString)
        for {
          _          <- ZStream.succeed(bucketName) >>> createBucket
          wasCreated <- ZStream(bucketName) >>> existsBucket
          _          <- ZStream.succeed(bucketName) >>> deleteEmptyBucket
          wasDeleted <- (ZStream(bucketName) >>> existsBucket).map(!_)
        } yield assertTrue(wasCreated) && assertTrue(wasDeleted)
      }
    )

  private lazy val deleteBucketSuite =
    suite("deleteBucket")(
      test("fails if bucket is not empty") {
        val bucketName = BucketName(UUID.randomUUID().toString)
        for {
          _          <- ZStream.succeed(bucketName) >>> createBucket
          wasCreated <- ZStream(bucketName) >>> existsBucket
          _ <- ZStream.fromChunk[Byte](Chunk(1, 2, 3)) >>> putObject(
                 bucketName,
                 ObjectKey(UUID.randomUUID().toString)
               )
          wasDeleted <-
            (ZStream.succeed(bucketName) >>> deleteEmptyBucket).as(true).catchSome { case _: AwsError =>
              ZIO.succeed(false)
            }
        } yield assertTrue(wasCreated) && assert(wasDeleted)(equalTo(false))
      },
      test("fails if bucket not exists") {
        val bucketName = BucketName(UUID.randomUUID().toString)
        for {
          wasCreated <- ZStream(bucketName) >>> existsBucket
          deleteFails <-
            (ZStream.succeed(bucketName) >>> deleteEmptyBucket).as(false).catchSome { case _: AwsError =>
              ZIO.succeed(true)
            }
        } yield assert(wasCreated)(equalTo(false)) && assertTrue(deleteFails)
      },
      test("succeeds if bucket is empty") {
        val bucketName = BucketName(UUID.randomUUID().toString)
        for {
          _          <- ZStream.succeed(bucketName) >>> createBucket
          wasCreated <- ZStream(bucketName) >>> existsBucket
          wasDeleted <- (ZStream.succeed(bucketName) >>> deleteEmptyBucket).as(true)
        } yield assertTrue(wasCreated) && assertTrue(wasDeleted)
      }
    )

  private lazy val deleteObjectsSuite =
    suite("deleteObjects")(
      test("succeeds if object not exists") {
        val bucketName = BucketName(UUID.randomUUID().toString)
        val key        = ObjectKey(UUID.randomUUID().toString)
        for {
          _                          <- ZStream.succeed(bucketName) >>> createBucket
          objectExistsBeforeDeletion <- ZStream(key) >>> existsObject(bucketName)
          _                          <- ZStream(key) >>> deleteObjects(bucketName)
        } yield assert(objectExistsBeforeDeletion)(equalTo(false))
      },
      test("succeeds") {
        val bucketName = BucketName(UUID.randomUUID().toString)
        val key1       = ObjectKey(UUID.randomUUID().toString)
        val key2       = ObjectKey(UUID.randomUUID().toString)
        val key3       = ObjectKey(UUID.randomUUID().toString)
        for {
          _                           <- ZStream.succeed(bucketName) >>> createBucket
          _                           <- ZStream.fromChunk[Byte](Chunk(1, 2, 3)) >>> putObject(bucketName, key1)
          _                           <- ZStream.fromChunk[Byte](Chunk(1)) >>> putObject(bucketName, key2)
          _                           <- ZStream.fromChunk[Byte](Chunk(2)) >>> putObject(bucketName, key3)
          object1ExistsBeforeDeletion <- ZStream(key1) >>> existsObject(bucketName)
          object2ExistsBeforeDeletion <- ZStream(key2) >>> existsObject(bucketName)
          object3ExistsBeforeDeletion <- ZStream(key3) >>> existsObject(bucketName)
          objectsExistBeforeDeletion =
            object1ExistsBeforeDeletion && object2ExistsBeforeDeletion && object3ExistsBeforeDeletion
          _                          <- ZStream(key1, key2, key3) >>> deleteObjects(bucketName)
          object1ExistsAfterDeletion <- ZStream(key1) >>> existsObject(bucketName)
          object2ExistsAfterDeletion <- ZStream(key2) >>> existsObject(bucketName)
          object3ExistsAfterDeletion <- ZStream(key3) >>> existsObject(bucketName)
          objectsExistAfterDeletion =
            object1ExistsAfterDeletion || object2ExistsAfterDeletion || object3ExistsAfterDeletion
        } yield assertTrue(objectsExistBeforeDeletion) && assert(objectsExistAfterDeletion)(equalTo(false))
      }
    )

  private lazy val listObjectsSuite =
    suite("listObjects")(
      test("fails when bucket doesn't exist") {
        val bucketName = BucketName(UUID.randomUUID().toString)
        for {
          exit <- listObjects(bucketName).runCollect.exit
          failsWithExpectedError <-
            exit.as(false).catchSome { case _: AwsError => ZIO.succeed(true) }
        } yield assertTrue(failsWithExpectedError)
      },
      test("succeeds") {
        val bucketName = BucketName(UUID.randomUUID().toString)
        val obj1       = ObjectKey(UUID.randomUUID().toString)
        val obj2       = ObjectKey(UUID.randomUUID().toString)
        for {
          _                   <- ZStream.succeed(bucketName) >>> createBucket
          testData            <- Random.nextBytes(5)
          _                   <- ZStream.fromChunk(testData) >>> putObject(bucketName, obj1)
          _                   <- ZStream.fromChunk(testData) >>> putObject(bucketName, obj2)
          actual              <- listObjects(bucketName).runCollect
          _                   <- ZStream.fromChunk(Chunk(obj1, obj2)) >>> deleteObjects(bucketName)
          afterObjectDeletion <- listObjects(bucketName).runCollect
        } yield assertTrue(actual.sortBy(_.toString) == Chunk(obj1, obj2).sortBy(_.toString)) && assertTrue(
          afterObjectDeletion.isEmpty
        )
      }
    )

  private lazy val moveObjectSuite =
    suite("moveObject")(
      test("moves preexisting object") {
        val bucket1 = BucketName(UUID.randomUUID().toString)
        val bucket2 = BucketName(UUID.randomUUID().toString)
        val key     = ObjectKey(UUID.randomUUID().toString)
        for {
          _             <- ZStream(bucket1, bucket2) >>> createBucket
          content1      <- Random.nextBytes(5)
          content2      <- Random.nextBytes(5)
          _             <- ZStream.fromChunk(content1) >>> putObject(bucket1, key)
          _             <- ZStream.fromChunk(content2) >>> putObject(bucket2, key)
          _             <- ZStream(S3Connector.MoveObject(bucket1, key, bucket2, key)) >>> moveObject
          copiedContent <- getObject(bucket2, key).runCollect
        } yield assertTrue(copiedContent == content1)
      },
      test("succeeds") {

        val bucket1 = BucketName(UUID.randomUUID().toString)
        val bucket2 = BucketName(UUID.randomUUID().toString)
        val key1    = ObjectKey(UUID.randomUUID().toString)
        val key2    = ObjectKey(UUID.randomUUID().toString)
        val key3    = ObjectKey(UUID.randomUUID().toString)
        val key4    = ObjectKey(UUID.randomUUID().toString)

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

        } yield assertTrue(initialB1Objects.sortBy(_.toString) == Chunk(key1, key2, key3).sortBy(_.toString)) &&
          assertTrue(initialB2Objects.isEmpty) &&
          assertTrue(b1Objects == Chunk(key1)) &&
          assertTrue(b2Objects.sortBy(_.toString) == Chunk(key1, key2, key4).sortBy(_.toString))
      }
    )

  private lazy val putObjectSuite =
    suite("putObject")(
      test("succeeds") {
        val bucketName = BucketName(UUID.randomUUID().toString)
        val objectKey  = ObjectKey(UUID.randomUUID().toString)
        for {
          _        <- ZStream(bucketName) >>> createBucket
          testData <- Random.nextBytes(5)
          _        <- ZStream.fromChunk(testData) >>> putObject(bucketName, objectKey)
          actual   <- getObject(bucketName, objectKey).runCollect
        } yield assertTrue(actual == testData)
      }
    )

}
