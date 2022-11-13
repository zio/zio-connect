package zio.connect.s3

import zio.connect.s3.S3Connector.{BucketName, CopyObject, ObjectKey, S3Exception}
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._
import zio.{Chunk, Random, ZIO}

import java.util.UUID

trait S3ConnectorSpec extends ZIOSpecDefault {

  private val usEast1Region = "us-east-1"
  private val usWest2Region = "us-west-2"

  val s3ConnectorSpec =
    copyObjectSpec + createBucketSuite + deleteBucketSuite + deleteObjectsSuite + listObjectsSuite + moveObjectSuite + putObjectSuite

  private lazy val copyObjectSpec =
    suite("copyObject")(
      test("replaces preexisting object") {
        val bucket1 = BucketName(UUID.randomUUID().toString)
        val bucket2 = BucketName(UUID.randomUUID().toString)
        val key     = ObjectKey(UUID.randomUUID().toString)
        for {
          _             <- ZStream(bucket1, bucket2) >>> createBucket(usEast1Region)
          content1      <- Random.nextBytes(5)
          content2      <- Random.nextBytes(5)
          _             <- ZStream.fromChunk(content1) >>> putObject(bucket1, key, usEast1Region)
          _             <- ZStream.fromChunk(content2) >>> putObject(bucket2, key, usEast1Region)
          _             <- ZStream(S3Connector.CopyObject(bucket1, key, bucket2)) >>> copyObject(usEast1Region)
          copiedContent <- getObject(bucket2, key, usEast1Region).runCollect
        } yield assertTrue(copiedContent == content1)
      },
      test("succeeds") {
        val bucket1 = BucketName(UUID.randomUUID().toString)
        val object1 = ObjectKey(UUID.randomUUID().toString)
        val object2 = ObjectKey(UUID.randomUUID().toString)
        val bucket2 = BucketName(UUID.randomUUID().toString)
        for {
          _            <- ZStream(bucket1, bucket2) >>> createBucket(usEast1Region)
          o1Content    <- Random.nextBytes(5)
          o2Content    <- Random.nextBytes(5)
          _            <- ZStream.fromChunk(o1Content) >>> putObject(bucket1, object1, usEast1Region)
          _            <- ZStream.fromChunk(o2Content) >>> putObject(bucket1, object2, usEast1Region)
          initialFiles <- listObjects(bucket1, usEast1Region).runCollect
          _ <- ZStream(CopyObject(bucket1, object1, bucket2), CopyObject(bucket1, object2, bucket2)) >>> copyObject(
                 usEast1Region
               )
          copiedFiles    <- listObjects(bucket2, usEast1Region).runCollect
          copiedContent1 <- getObject(bucket2, object1, usEast1Region).runCollect
          copiedContent2 <- getObject(bucket2, object2, usEast1Region).runCollect

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
          _             <- ZStream(bucket1) >>> createBucket(usEast1Region)
          _             <- ZStream(bucket2) >>> createBucket(usWest2Region)
          content1      <- Random.nextBytes(5)
          content2      <- Random.nextBytes(5)
          _             <- ZStream.fromChunk(content1) >>> putObject(bucket1, key, usEast1Region)
          _             <- ZStream.fromChunk(content2) >>> putObject(bucket2, key, usWest2Region)
          _             <- ZStream(S3Connector.CopyObject(bucket1, key, bucket2)) >>> copyObject(usEast1Region, usWest2Region)
          copiedContent <- getObject(bucket2, key, usWest2Region).runCollect
        } yield assertTrue(copiedContent == content1)
      }
    )

  private lazy val createBucketSuite: Spec[S3Connector, S3Exception] =
    suite("createBucket")(
      test("changes nothing if bucket already exists") {
        val bucketName = BucketName(UUID.randomUUID().toString)
        for {
          _       <- ZStream(bucketName) >>> createBucket(usEast1Region)
          content <- Random.nextBytes(5)
          _ <-
            ZStream.fromChunk(content) >>> putObject(bucketName, ObjectKey(UUID.randomUUID().toString), usEast1Region)
          objectsBefore <- listObjects(bucketName, usEast1Region).runCollect
          _             <- ZStream(bucketName) >>> createBucket(usEast1Region)
          objectsAfter  <- listObjects(bucketName, usEast1Region).runCollect
        } yield assertTrue(objectsBefore == objectsAfter)
      },
      test("succeeds") {
        val bucketName = BucketName(UUID.randomUUID().toString)
        for {
          _          <- ZStream.succeed(bucketName) >>> createBucket(usEast1Region)
          wasCreated <- ZStream(bucketName) >>> existsBucket(usEast1Region)
          _          <- ZStream.succeed(bucketName) >>> deleteEmptyBucket(usEast1Region)
          wasDeleted <- (ZStream(bucketName) >>> existsBucket(usEast1Region)).map(!_)
        } yield assertTrue(wasCreated) && assertTrue(wasDeleted)
      }
    )

  private lazy val deleteBucketSuite: Spec[S3Connector, S3Exception] =
    suite("deleteBucket")(
      test("fails if bucket is not empty") {
        val bucketName = BucketName(UUID.randomUUID().toString)
        for {
          _          <- ZStream.succeed(bucketName) >>> createBucket(usEast1Region)
          wasCreated <- ZStream(bucketName) >>> existsBucket(usEast1Region)
          _ <- ZStream.fromChunk[Byte](Chunk(1, 2, 3)) >>> putObject(
                 bucketName,
                 ObjectKey(UUID.randomUUID().toString),
                 usEast1Region
               )
          wasDeleted <-
            (ZStream.succeed(bucketName) >>> deleteEmptyBucket(usEast1Region)).as(true).catchSome {
              case _: S3Exception =>
                ZIO.succeed(false)
            }
        } yield assertTrue(wasCreated) && assert(wasDeleted)(equalTo(false))
      },
      test("fails if bucket not exists") {
        val bucketName = BucketName(UUID.randomUUID().toString)
        for {
          wasCreated <- ZStream(bucketName) >>> existsBucket(usEast1Region)
          deleteFails <-
            (ZStream.succeed(bucketName) >>> deleteEmptyBucket(usEast1Region)).as(false).catchSome {
              case _: S3Exception =>
                ZIO.succeed(true)
            }
        } yield assert(wasCreated)(equalTo(false)) && assertTrue(deleteFails)
      },
      test("succeeds if bucket is empty") {
        val bucketName = BucketName(UUID.randomUUID().toString)
        for {
          _          <- ZStream.succeed(bucketName) >>> createBucket(usEast1Region)
          wasCreated <- ZStream(bucketName) >>> existsBucket(usEast1Region)
          wasDeleted <- (ZStream.succeed(bucketName) >>> deleteEmptyBucket(usEast1Region)).as(true)
        } yield assertTrue(wasCreated) && assertTrue(wasDeleted)
      }
    )

  private lazy val deleteObjectsSuite: Spec[S3Connector, S3Exception] =
    suite("deleteObjects")(
      test("succeeds if object not exists") {
        val bucketName = BucketName(UUID.randomUUID().toString)
        val key        = ObjectKey(UUID.randomUUID().toString)
        for {
          _                          <- ZStream.succeed(bucketName) >>> createBucket(usEast1Region)
          objectExistsBeforeDeletion <- ZStream(key) >>> existsObject(bucketName, usEast1Region)
          _                          <- ZStream(key) >>> deleteObjects(bucketName, usEast1Region)
        } yield assert(objectExistsBeforeDeletion)(equalTo(false))
      },
      test("succeeds") {
        val bucketName = BucketName(UUID.randomUUID().toString)
        val key1       = ObjectKey(UUID.randomUUID().toString)
        val key2       = ObjectKey(UUID.randomUUID().toString)
        val key3       = ObjectKey(UUID.randomUUID().toString)
        for {
          _                           <- ZStream.succeed(bucketName) >>> createBucket(usEast1Region)
          _                           <- ZStream.fromChunk[Byte](Chunk(1, 2, 3)) >>> putObject(bucketName, key1, usEast1Region)
          _                           <- ZStream.fromChunk[Byte](Chunk(1)) >>> putObject(bucketName, key2, usEast1Region)
          _                           <- ZStream.fromChunk[Byte](Chunk(2)) >>> putObject(bucketName, key3, usEast1Region)
          object1ExistsBeforeDeletion <- ZStream(key1) >>> existsObject(bucketName, usEast1Region)
          object2ExistsBeforeDeletion <- ZStream(key2) >>> existsObject(bucketName, usEast1Region)
          object3ExistsBeforeDeletion <- ZStream(key3) >>> existsObject(bucketName, usEast1Region)
          objectsExistBeforeDeletion =
            object1ExistsBeforeDeletion && object2ExistsBeforeDeletion && object3ExistsBeforeDeletion
          _                          <- ZStream(key1, key2, key3) >>> deleteObjects(bucketName, usEast1Region)
          object1ExistsAfterDeletion <- ZStream(key1) >>> existsObject(bucketName, usEast1Region)
          object2ExistsAfterDeletion <- ZStream(key2) >>> existsObject(bucketName, usEast1Region)
          object3ExistsAfterDeletion <- ZStream(key3) >>> existsObject(bucketName, usEast1Region)
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
          exit <- listObjects(bucketName, usEast1Region).runCollect.exit
          failsWithExpectedError <-
            exit.as(false).catchSome { case S3Exception(_) => ZIO.succeed(true) }
        } yield assertTrue(failsWithExpectedError)
      },
      test("succeeds") {
        val bucketName = BucketName(UUID.randomUUID().toString)
        val obj1       = ObjectKey(UUID.randomUUID().toString)
        val obj2       = ObjectKey(UUID.randomUUID().toString)
        for {
          _                   <- ZStream.succeed(bucketName) >>> createBucket(usEast1Region)
          testData            <- Random.nextBytes(5)
          _                   <- ZStream.fromChunk(testData) >>> putObject(bucketName, obj1, usEast1Region)
          _                   <- ZStream.fromChunk(testData) >>> putObject(bucketName, obj2, usEast1Region)
          actual              <- listObjects(bucketName, usEast1Region).runCollect
          _                   <- ZStream.fromChunk(Chunk(obj1, obj2)) >>> deleteObjects(bucketName, usEast1Region)
          afterObjectDeletion <- listObjects(bucketName, usEast1Region).runCollect
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
          _             <- ZStream(bucket1, bucket2) >>> createBucket(usEast1Region)
          content1      <- Random.nextBytes(5)
          content2      <- Random.nextBytes(5)
          _             <- ZStream.fromChunk(content1) >>> putObject(bucket1, key, usEast1Region)
          _             <- ZStream.fromChunk(content2) >>> putObject(bucket2, key, usEast1Region)
          _             <- ZStream(S3Connector.MoveObject(bucket1, key, bucket2, key)) >>> moveObject(usEast1Region)
          copiedContent <- getObject(bucket2, key, usEast1Region).runCollect
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
          _                <- ZStream(bucket1, bucket2) >>> createBucket(usEast1Region)
          testDataK1       <- Random.nextBytes(5)
          _                <- ZStream.fromChunk(testDataK1) >>> putObject(bucket1, key1, usEast1Region)
          testDataK2       <- Random.nextBytes(5)
          _                <- ZStream.fromChunk(testDataK2) >>> putObject(bucket1, key2, usEast1Region)
          testDataK3       <- Random.nextBytes(5)
          _                <- ZStream.fromChunk(testDataK3) >>> putObject(bucket1, key3, usEast1Region)
          initialB1Objects <- listObjects(bucket1, usEast1Region).runCollect
          initialB2Objects <- listObjects(bucket2, usEast1Region).runCollect
          _                <- ZStream(S3Connector.CopyObject(bucket1, key1, bucket2)) >>> copyObject(usEast1Region)
          _                <- ZStream(S3Connector.MoveObject(bucket1, key2, bucket2, key2)) >>> moveObject(usEast1Region)
          _                <- ZStream(S3Connector.MoveObject(bucket1, key3, bucket2, key4)) >>> moveObject(usEast1Region)
          b1Objects        <- listObjects(bucket1, usEast1Region).runCollect
          b2Objects        <- listObjects(bucket2, usEast1Region).runCollect

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
          _             <- ZStream(bucket1) >>> createBucket(usEast1Region)
          _             <- ZStream(bucket2) >>> createBucket(usWest2Region)
          content1      <- Random.nextBytes(5)
          content2      <- Random.nextBytes(5)
          _             <- ZStream.fromChunk(content1) >>> putObject(bucket1, key, usEast1Region)
          _             <- ZStream.fromChunk(content2) >>> putObject(bucket2, key, usWest2Region)
          _             <- ZStream(S3Connector.MoveObject(bucket1, key, bucket2, key)) >>> moveObject(usEast1Region, usWest2Region)
          copiedContent <- getObject(bucket2, key, usWest2Region).runCollect
        } yield assertTrue(copiedContent == content1)
      }
    )

  private lazy val putObjectSuite: Spec[S3Connector, S3Exception] =
    suite("putObject")(
      test("succeeds") {
        val bucketName = BucketName(UUID.randomUUID().toString)
        val objectKey  = ObjectKey(UUID.randomUUID().toString)
        for {
          _        <- ZStream(bucketName) >>> createBucket(usEast1Region)
          testData <- Random.nextBytes(5)
          _        <- ZStream.fromChunk(testData) >>> putObject(bucketName, objectKey, usEast1Region)
          actual   <- getObject(bucketName, objectKey, usEast1Region).runCollect
        } yield assertTrue(actual == testData)
      }
    )

}
