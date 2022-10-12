package zio.connect.s3

import zio.connect.s3.S3Connector.S3Exception
import zio.stream.ZStream
import zio.test._

trait S3ConnectorSpec extends ZIOSpecDefault {

  val s3ConnectorSpec = createBucketSuite

  private lazy val createBucketSuite: Spec[S3Connector, S3Exception] =
    suite("createBucket") {
      test("succeeds") {
        val bucketName = "my-test-bucket"
        for {
          _          <- ZStream.succeed(bucketName) >>> createBucket
          wasCreated <- existsBucket(bucketName).runHead.map(_.contains(true))
          _          <- ZStream.succeed(bucketName) >>> deleteEmptyBuckets
          wasDeleted <- existsBucket(bucketName).runHead.map(_.contains(false))
        } yield assertTrue(wasCreated) && assertTrue(wasDeleted)
      }
    }

}
