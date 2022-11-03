import zio._
import zio.aws.core.config.AwsConfig
import zio.aws.netty.NettyHttpClient
import zio.aws.s3.S3
import zio.connect.s3.S3Connector._
import zio.connect.s3._
import zio.stream._

import java.nio.charset.StandardCharsets

object Example1 extends ZIOAppDefault {

  // Please read https://zio.github.io/zio-aws/docs/overview/overview_config to learn more about configuring/authenticating zio-aws
  // this configuration will work provided you have default aws credentials, i.e. access key and secret key in your `.aws` directory
  lazy val zioAwsConfig = NettyHttpClient.default >>> AwsConfig.default

  // Program does the following:
  // 1. Creates two random buckets
  // 2. Puts a quote as a text file into one bucket
  // 3. Copies that to another,
  // 4. Lists the objects in those buckets
  // 5. Gets the quote back from the second bucket
  // 6. Deletes the objects in both buckets
  // 7. Checks for the existence of the objects in both buckets
  // 8. Deletes the buckets provided they are empty
  val program: ZIO[S3Connector, Object, String] = {
    for {
      bucket1          <- Random.nextUUID.map(_.toString).map(uuid => BucketName(s"zio-connect-s3-bucket-$uuid"))
      bucket2          <- Random.nextUUID.map(_.toString).map(uuid => BucketName(s"zio-connect-s3-bucket-$uuid"))
      _                <- ZStream(bucket1, bucket2).run(createBucket)
      buckets          <- listBuckets.runCollect
      objectKey         = ObjectKey("quote.txt")
      _                <- ZStream.fromIterable(quote.getBytes(StandardCharsets.UTF_8)).run(putObject(bucket1, objectKey))
      _                <- ZStream(CopyObject(bucket1, objectKey, bucket2)).run(copyObject)
      objectsPerBucket <- ZIO.foreach(buckets)(bucket => listObjects(bucket).runCollect.map((bucket, _)))
      _ <- ZIO.foreach(objectsPerBucket) { case (bucket, objects) =>
             Console.printLine(s"Objects in bucket $bucket: ${objects.mkString}")
           }
      text            <- getObject(bucket2, objectKey) >>> ZPipeline.utf8Decode >>> ZSink.mkString
      _               <- ZIO.foreachPar(buckets)(bucket => ZStream(objectKey).run(deleteObjects(bucket)))
      bucketsNonEmpty <- ZIO.foreachPar(buckets)(bucket => ZStream(objectKey).run(existsObject(bucket)))
      _ <- ZStream
             .fromChunk(buckets)
             .run(deleteEmptyBucket)
             .when(bucketsNonEmpty.forall(_ == false))
             .orElseFail(new RuntimeException("Could not delete non-empty buckets"))
    } yield text
  }

  override def run: ZIO[Any with ZIOAppArgs with Scope, Object, String] =
    program
      .provide(zioAwsConfig, S3.live, s3ConnectorLiveLayer)
      .tapBoth(
        error => Console.printLine(s"error: ${error}"),
        text => Console.printLine(s"${text} ==\n ${quote}\nis ${text == quote}")
      )

  private def quote =
    "You should give up looking for lost cats and start searching for the other half of your shadow"

}
