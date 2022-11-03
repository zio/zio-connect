import zio._
import zio.aws.core.config.AwsConfig
import zio.aws.netty.NettyHttpClient
import zio.aws.s3.S3
import zio.connect.s3.S3Connector._
import zio.connect.s3._
import zio.stream._

object Example2 extends ZIOAppDefault {

  lazy val zioAwsConfig = NettyHttpClient.default >>> AwsConfig.default

  val bucketName = BucketName("this-very-charming-bucket-name") // BucketName is a zio prelude newtype of String

  val program1: ZIO[S3Connector, S3Exception, Unit] =
    for {
      _ <- ZStream(bucketName) >>> createBucket
    } yield ()

  val objectKey = ObjectKey("my-object") // ObjectKey is a zio prelude newtype of String

  val program2: ZIO[S3Connector, S3Exception, Unit] =
    for {
      content <- Random.nextString(100).map(_.getBytes).map(Chunk.fromArray)
      _       <- ZStream.fromChunk(content) >>> putObject(bucketName, objectKey)
    } yield ()

  val program3: ZIO[S3Connector, S3Exception, Chunk[ObjectKey]] =
    for {
      keys <- listObjects(bucketName).runCollect
    } yield keys

  val program4: ZIO[S3Connector, Object, String] =
    for {
      content <- getObject(bucketName, objectKey) >>> ZPipeline.utf8Decode >>> ZSink.mkString
    } yield content

  val program5: ZIO[S3Connector, S3Exception, Unit] =
    for {
      _ <- ZStream(objectKey) >>> deleteObjects(bucketName)
      _ <- ZStream(bucketName) >>> deleteEmptyBucket
    } yield ()

  def run: ZIO[ZIOAppArgs, Any, Any] =
    (program1 *> program2 *> program3 *> program4 <* program5)
      .provide(zioAwsConfig, S3.live, s3ConnectorLiveLayer)
      .tap(text => Console.printLine(s"content: ${text}"))
}
