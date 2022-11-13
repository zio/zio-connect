package zio.connect.s3.multiregion

import software.amazon.awssdk.regions.Region
import zio.aws.core.AwsError
import zio.aws.s3.model.primitives.{BucketName, ObjectKey}
import zio.connect.s3.S3Connector
import zio.connect.s3.S3Connector.CopyObject
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._
import zio.{Chunk, Random, ZIO}

import java.util.UUID

trait MultiRegionS3ConnectorSpec extends ZIOSpecDefault {

  val s3ConnectorSpec =
    copyObjectSpec + createBucketSuite + deleteBucketSuite + deleteObjectsSuite + listObjectsSuite + moveObjectSuite + putObjectSuite

  private lazy val copyObjectSpec =
    suite("copyObject")(
      test("replaces preexisting object") {
        val bucket1 = BucketName(UUID.randomUUID().toString)
        val bucket2 = BucketName(UUID.randomUUID().toString)
        val key     = ObjectKey(UUID.randomUUID().toString)
        for {
          _             <- ZStream(bucket1, bucket2) >>> createBucket(Region.US_EAST_1)
          content1      <- Random.nextBytes(5)
          content2      <- Random.nextBytes(5)
          _             <- ZStream.fromChunk(content1) >>> putObject(bucket1, key, Region.US_EAST_1)
          _             <- ZStream.fromChunk(content2) >>> putObject(bucket2, key, Region.US_EAST_1)
          _             <- ZStream(S3Connector.CopyObject(bucket1, key, bucket2)) >>> copyObject(Region.US_EAST_1)
          copiedContent <- getObject(bucket2, key, Region.US_EAST_1).runCollect
        } yield assertTrue(copiedContent == content1)
      },
      test("succeeds") {
        val region  = Region.US_EAST_1
        val bucket1 = BucketName(UUID.randomUUID().toString)
        val object1 = ObjectKey(UUID.randomUUID().toString)
        val object2 = ObjectKey(UUID.randomUUID().toString)
        val bucket2 = BucketName(UUID.randomUUID().toString)
        for {
          _            <- ZStream(bucket1, bucket2) >>> createBucket(region)
          o1Content    <- Random.nextBytes(5)
          o2Content    <- Random.nextBytes(5)
          _            <- ZStream.fromChunk(o1Content) >>> putObject(bucket1, object1, region)
          _            <- ZStream.fromChunk(o2Content) >>> putObject(bucket1, object2, region)
          initialFiles <- listObjects(bucket1, region).runCollect
          _ <- ZStream(CopyObject(bucket1, object1, bucket2), CopyObject(bucket1, object2, bucket2)) >>> copyObject(
                 region
               )
          copiedFiles    <- listObjects(bucket2, region).runCollect
          copiedContent1 <- getObject(bucket2, object1, region).runCollect
          copiedContent2 <- getObject(bucket2, object2, region).runCollect

          filesWereCopied =
            Chunk(object1, object2).sortBy(_.toString) == initialFiles.sortBy(_.toString) &&
              initialFiles.sortBy(_.toString) == copiedFiles.sortBy(_.toString)
          o1CopyMatchesOriginal = o1Content == copiedContent1
          o2CopyMatchesOriginal = o2Content == copiedContent2
        } yield assertTrue(filesWereCopied) && assertTrue(o1CopyMatchesOriginal) && assertTrue(o2CopyMatchesOriginal)
      },
      test("replaces preexisting object cross region") {
        val bucket1 = BucketName(UUID.randomUUID().toString)
        val bucket2 = BucketName(UUID.randomUUID().toString)
        val key     = ObjectKey(UUID.randomUUID().toString)
        for {
          _             <- ZStream(bucket1) >>> createBucket(Region.US_EAST_1)
          _             <- ZStream(bucket2) >>> createBucket(Region.US_WEST_2)
          content1      <- Random.nextBytes(5)
          content2      <- Random.nextBytes(5)
          _             <- ZStream.fromChunk(content1) >>> putObject(bucket1, key, Region.US_EAST_1)
          _             <- ZStream.fromChunk(content2) >>> putObject(bucket2, key, Region.US_WEST_2)
          _             <- ZStream(S3Connector.CopyObject(bucket1, key, bucket2)) >>> copyObject(Region.US_EAST_1, Region.US_WEST_2)
          copiedContent <- getObject(bucket2, key, Region.US_WEST_2).runCollect
        } yield assertTrue(copiedContent == content1)
      }
    )

  private lazy val createBucketSuite =
    suite("createBucket")(
      test("changes nothing if bucket already exists") {
        val bucketName = BucketName(UUID.randomUUID().toString)
        for {
          _       <- ZStream(bucketName) >>> createBucket(Region.US_EAST_1)
          content <- Random.nextBytes(5)
          _ <-
            ZStream
              .fromChunk(content) >>> putObject(bucketName, ObjectKey(UUID.randomUUID().toString), Region.US_EAST_1)
          objectsBefore <- listObjects(bucketName, Region.US_EAST_1).runCollect
          _             <- ZStream(bucketName) >>> createBucket(Region.US_EAST_1)
          objectsAfter  <- listObjects(bucketName, Region.US_EAST_1).runCollect
        } yield assertTrue(objectsBefore == objectsAfter)
      },
      test("succeeds") {
        val bucketName = BucketName(UUID.randomUUID().toString)
        for {
          _          <- ZStream.succeed(bucketName) >>> createBucket(Region.US_EAST_1)
          wasCreated <- ZStream(bucketName) >>> existsBucket(Region.US_EAST_1)
          _          <- ZStream.succeed(bucketName) >>> deleteEmptyBucket(Region.US_EAST_1)
          wasDeleted <- (ZStream(bucketName) >>> existsBucket(Region.US_EAST_1)).map(!_)
        } yield assertTrue(wasCreated) && assertTrue(wasDeleted)
      }
    )

  private lazy val deleteBucketSuite =
    suite("deleteBucket")(
      test("fails if bucket is not empty") {
        val bucketName = BucketName(UUID.randomUUID().toString)
        for {
          _          <- ZStream.succeed(bucketName) >>> createBucket(Region.US_EAST_1)
          wasCreated <- ZStream(bucketName) >>> existsBucket(Region.US_EAST_1)
          _ <- ZStream.fromChunk[Byte](Chunk(1, 2, 3)) >>> putObject(
                 bucketName,
                 ObjectKey(UUID.randomUUID().toString),
                 Region.US_EAST_1
               )
          wasDeleted <-
            (ZStream.succeed(bucketName) >>> deleteEmptyBucket(Region.US_EAST_1)).as(true).catchSome {
              case _: AwsError =>
                ZIO.succeed(false)
            }
        } yield assertTrue(wasCreated) && assert(wasDeleted)(equalTo(false))
      },
      test("fails if bucket not exists") {
        val bucketName = BucketName(UUID.randomUUID().toString)
        for {
          wasCreated <- ZStream(bucketName) >>> existsBucket(Region.US_EAST_1)
          deleteFails <-
            (ZStream.succeed(bucketName) >>> deleteEmptyBucket(Region.US_EAST_1)).as(false).catchSome {
              case _: AwsError =>
                ZIO.succeed(true)
            }
        } yield assert(wasCreated)(equalTo(false)) && assertTrue(deleteFails)
      },
      test("succeeds if bucket is empty") {
        val bucketName = BucketName(UUID.randomUUID().toString)
        for {
          _          <- ZStream.succeed(bucketName) >>> createBucket(Region.US_EAST_1)
          wasCreated <- ZStream(bucketName) >>> existsBucket(Region.US_EAST_1)
          wasDeleted <- (ZStream.succeed(bucketName) >>> deleteEmptyBucket(Region.US_EAST_1)).as(true)
        } yield assertTrue(wasCreated) && assertTrue(wasDeleted)
      }
    )

  private lazy val deleteObjectsSuite =
    suite("deleteObjects")(
      test("succeeds if object not exists") {
        val bucketName = BucketName(UUID.randomUUID().toString)
        val key        = ObjectKey(UUID.randomUUID().toString)
        for {
          _                          <- ZStream.succeed(bucketName) >>> createBucket(Region.US_EAST_1)
          objectExistsBeforeDeletion <- ZStream(key) >>> existsObject(bucketName, Region.US_EAST_1)
          _                          <- ZStream(key) >>> deleteObjects(bucketName, Region.US_EAST_1)
        } yield assert(objectExistsBeforeDeletion)(equalTo(false))
      },
      test("succeeds") {
        val bucketName = BucketName(UUID.randomUUID().toString)
        val key1       = ObjectKey(UUID.randomUUID().toString)
        val key2       = ObjectKey(UUID.randomUUID().toString)
        val key3       = ObjectKey(UUID.randomUUID().toString)
        for {
          _                           <- ZStream.succeed(bucketName) >>> createBucket(Region.US_EAST_1)
          _                           <- ZStream.fromChunk[Byte](Chunk(1, 2, 3)) >>> putObject(bucketName, key1, Region.US_EAST_1)
          _                           <- ZStream.fromChunk[Byte](Chunk(1)) >>> putObject(bucketName, key2, Region.US_EAST_1)
          _                           <- ZStream.fromChunk[Byte](Chunk(2)) >>> putObject(bucketName, key3, Region.US_EAST_1)
          object1ExistsBeforeDeletion <- ZStream(key1) >>> existsObject(bucketName, Region.US_EAST_1)
          object2ExistsBeforeDeletion <- ZStream(key2) >>> existsObject(bucketName, Region.US_EAST_1)
          object3ExistsBeforeDeletion <- ZStream(key3) >>> existsObject(bucketName, Region.US_EAST_1)
          objectsExistBeforeDeletion =
            object1ExistsBeforeDeletion && object2ExistsBeforeDeletion && object3ExistsBeforeDeletion
          _                          <- ZStream(key1, key2, key3) >>> deleteObjects(bucketName, Region.US_EAST_1)
          object1ExistsAfterDeletion <- ZStream(key1) >>> existsObject(bucketName, Region.US_EAST_1)
          object2ExistsAfterDeletion <- ZStream(key2) >>> existsObject(bucketName, Region.US_EAST_1)
          object3ExistsAfterDeletion <- ZStream(key3) >>> existsObject(bucketName, Region.US_EAST_1)
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
          exit <- listObjects(bucketName, Region.US_EAST_1).runCollect.exit
          failsWithExpectedError <-
            exit.as(false).catchSome { case _: AwsError => ZIO.succeed(true) }
        } yield assertTrue(failsWithExpectedError)
      },
      test("succeeds") {
        val bucketName = BucketName(UUID.randomUUID().toString)
        val obj1       = ObjectKey(UUID.randomUUID().toString)
        val obj2       = ObjectKey(UUID.randomUUID().toString)
        for {
          _                   <- ZStream.succeed(bucketName) >>> createBucket(Region.US_EAST_1)
          testData            <- Random.nextBytes(5)
          _                   <- ZStream.fromChunk(testData) >>> putObject(bucketName, obj1, Region.US_EAST_1)
          _                   <- ZStream.fromChunk(testData) >>> putObject(bucketName, obj2, Region.US_EAST_1)
          actual              <- listObjects(bucketName, Region.US_EAST_1).runCollect
          _                   <- ZStream.fromChunk(Chunk(obj1, obj2)) >>> deleteObjects(bucketName, Region.US_EAST_1)
          afterObjectDeletion <- listObjects(bucketName, Region.US_EAST_1).runCollect
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
          _             <- ZStream(bucket1, bucket2) >>> createBucket(Region.US_EAST_1)
          content1      <- Random.nextBytes(5)
          content2      <- Random.nextBytes(5)
          _             <- ZStream.fromChunk(content1) >>> putObject(bucket1, key, Region.US_EAST_1)
          _             <- ZStream.fromChunk(content2) >>> putObject(bucket2, key, Region.US_EAST_1)
          _             <- ZStream(S3Connector.MoveObject(bucket1, key, bucket2, key)) >>> moveObject(Region.US_EAST_1)
          copiedContent <- getObject(bucket2, key, Region.US_EAST_1).runCollect
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
          _                <- ZStream(bucket1, bucket2) >>> createBucket(Region.US_EAST_1)
          testDataK1       <- Random.nextBytes(5)
          _                <- ZStream.fromChunk(testDataK1) >>> putObject(bucket1, key1, Region.US_EAST_1)
          testDataK2       <- Random.nextBytes(5)
          _                <- ZStream.fromChunk(testDataK2) >>> putObject(bucket1, key2, Region.US_EAST_1)
          testDataK3       <- Random.nextBytes(5)
          _                <- ZStream.fromChunk(testDataK3) >>> putObject(bucket1, key3, Region.US_EAST_1)
          initialB1Objects <- listObjects(bucket1, Region.US_EAST_1).runCollect
          initialB2Objects <- listObjects(bucket2, Region.US_EAST_1).runCollect
          _                <- ZStream(S3Connector.CopyObject(bucket1, key1, bucket2)) >>> copyObject(Region.US_EAST_1)
          _                <- ZStream(S3Connector.MoveObject(bucket1, key2, bucket2, key2)) >>> moveObject(Region.US_EAST_1)
          _                <- ZStream(S3Connector.MoveObject(bucket1, key3, bucket2, key4)) >>> moveObject(Region.US_EAST_1)
          b1Objects        <- listObjects(bucket1, Region.US_EAST_1).runCollect
          b2Objects        <- listObjects(bucket2, Region.US_EAST_1).runCollect

        } yield assertTrue(initialB1Objects.sortBy(_.toString) == Chunk(key1, key2, key3).sortBy(_.toString)) &&
          assertTrue(initialB2Objects.isEmpty) &&
          assertTrue(b1Objects == Chunk(key1)) &&
          assertTrue(b2Objects.sortBy(_.toString) == Chunk(key1, key2, key4).sortBy(_.toString))

      },
      test("moves preexisting object cross region") {
        val bucket1 = BucketName(UUID.randomUUID().toString)
        val bucket2 = BucketName(UUID.randomUUID().toString)
        val key     = ObjectKey(UUID.randomUUID().toString)
        for {
          _        <- ZStream(bucket1) >>> createBucket(Region.US_EAST_1)
          _        <- ZStream(bucket2) >>> createBucket(Region.US_WEST_2)
          content1 <- Random.nextBytes(5)
          content2 <- Random.nextBytes(5)
          _        <- ZStream.fromChunk(content1) >>> putObject(bucket1, key, Region.US_EAST_1)
          _        <- ZStream.fromChunk(content2) >>> putObject(bucket2, key, Region.US_WEST_2)
          _ <- ZStream(S3Connector.MoveObject(bucket1, key, bucket2, key)) >>> moveObject(
                 Region.US_EAST_1,
                 Region.US_WEST_2
               )
          copiedContent <- getObject(bucket2, key, Region.US_WEST_2).runCollect
        } yield assertTrue(copiedContent == content1)
      }
    )

  private lazy val putObjectSuite =
    suite("putObject")(
      test("succeeds") {
        val bucketName = BucketName(UUID.randomUUID().toString)
        val objectKey  = ObjectKey(UUID.randomUUID().toString)
        for {
          _        <- ZStream(bucketName) >>> createBucket(Region.US_EAST_1)
          testData <- Random.nextBytes(5)
          _        <- ZStream.fromChunk(testData) >>> putObject(bucketName, objectKey, Region.US_EAST_1)
          actual   <- getObject(bucketName, objectKey, Region.US_EAST_1).runCollect
        } yield assertTrue(actual == testData)
      }
    )

}
