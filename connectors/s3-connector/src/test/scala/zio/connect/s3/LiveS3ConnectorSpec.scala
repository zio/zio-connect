package zio.connect.s3
import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.containers.localstack.LocalStackContainer.Service
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.{Region => AWSRegion}
import zio.aws.core.config.AwsConfig
import zio.aws.core.httpclient.HttpClient
import zio.aws.netty.NettyHttpClient
import zio.aws.s3.S3
import zio.connect.s3.S3Connector.Region
import zio.{Scope, ZIO, ZLayer}

object LiveS3ConnectorSpec extends S3ConnectorSpec {
  override def spec =
    suite("LiveS3ConnectorSpec")(s3ConnectorSpec)
      .provideSomeShared[Scope](
        localStackContainer,
        awsConfig,
        s3,
        zio.connect.s3.s3ConnectorLiveLayer
      )

  lazy val httpClient: ZLayer[Any, Throwable, HttpClient] = NettyHttpClient.default
  lazy val awsConfig: ZLayer[Any, Throwable, AwsConfig]   = httpClient >>> AwsConfig.default

  lazy val localStackContainer: ZLayer[Scope, Throwable, LocalStackContainer] =
    ZLayer.fromZIO(
      ZIO.acquireRelease(ZIO.attempt {
        val localstackImage = DockerImageName.parse("localstack/localstack:0.11.3")
        val localstack = new LocalStackContainer(localstackImage)
          .withServices(Service.S3)
        localstack.start()
        localstack
      })(ls => ZIO.attempt(ls.stop()).orDie)
    )

  lazy val s3 = {
    val res = for {
      localstack <- ZIO.service[LocalStackContainer]
      s3USWest2 <- ZIO
                     .service[S3]
                     .provideSome[AwsConfig](
                       S3.customized(
                         _.credentialsProvider(
                           StaticCredentialsProvider
                             .create(AwsBasicCredentials.create(localstack.getAccessKey, localstack.getSecretKey))
                         ).region(AWSRegion.US_WEST_2)
                           .endpointOverride(localstack.getEndpointOverride(Service.S3))
                       )
                     )
      s3USEast1 <- ZIO
                     .service[S3]
                     .provideSome[AwsConfig](
                       S3.customized(
                         _.credentialsProvider(
                           StaticCredentialsProvider
                             .create(AwsBasicCredentials.create(localstack.getAccessKey, localstack.getSecretKey))
                         ).region(AWSRegion.of(localstack.getRegion))
                           .endpointOverride(localstack.getEndpointOverride(Service.S3))
                       )
                     )

    } yield Map(Region("us-east-1") -> s3USEast1, Region("us-west-2") -> s3USWest2)
    ZLayer
      .fromZIO(res)
  }

}
