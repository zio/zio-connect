import software.amazon.awssdk.regions.Region
import zio._
import zio.aws.core.AwsError
import zio.aws.core.config.AwsConfig
import zio.aws.netty.NettyHttpClient
import zio.aws.s3.S3
import zio.aws.s3.model.primitives.{BucketName, ObjectKey}
import zio.connect.s3._
import zio.stream._

object Example2 extends ZIOAppDefault {

  lazy val zioAwsConfig = NettyHttpClient.default >>> AwsConfig.default
  lazy val region       = Region.US_EAST_1

  val bucketName = BucketName("this-very-charming-bucket-name") // BucketName is a zio prelude newtype of String

  val program1: ZIO[S3Connector, AwsError, Unit] =
    for {
      _ <- ZStream(bucketName) >>> createBucket(region)
    } yield ()

  val objectKey = ObjectKey("my-object") // ObjectKey is a zio prelude newtype of String

  val program2: ZIO[S3Connector, AwsError, Unit] =
    for {
      content <- Random.nextString(100).map(_.getBytes).map(Chunk.fromArray)
      _       <- ZStream.fromChunk(content) >>> putObject(bucketName, objectKey, region)
    } yield ()

  val program3: ZIO[S3Connector, AwsError, Chunk[ObjectKey]] =
    for {
      keys <- listObjects(bucketName, region).runCollect
    } yield keys

  val program4: ZIO[S3Connector, Object, String] =
    for {
      content <- getObject(bucketName, objectKey, region) >>> ZPipeline.utf8Decode >>> ZSink.mkString
    } yield content

  val program5: ZIO[S3Connector, AwsError, Unit] =
    for {
      _ <- ZStream(objectKey) >>> deleteObjects(bucketName, region)
      _ <- ZStream(bucketName) >>> deleteEmptyBucket(region)
    } yield ()

  def run: ZIO[ZIOAppArgs, Any, Any] =
    (program1 *> program2 *> program3 *> program4 <* program5)
      .provide(zioAwsConfig, S3.live, s3ConnectorLiveLayer)
      .tap(text => Console.printLine(s"content: ${text}"))
}
