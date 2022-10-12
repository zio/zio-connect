package zio.connect.s3
import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.containers.localstack.LocalStackContainer.Service
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import zio.{Scope, ZIO, ZLayer}
import zio.aws.core.config.AwsConfig
import zio.aws.netty.NettyHttpClient
import zio.aws.s3.S3

object LiveS3ConnectorSpec extends S3ConnectorSpec {
  override def spec =
    suite("LiveS3ConnectorSpec")(s3ConnectorSpec)
      .provideSome[Scope](
        localStackContainer,
        awsConfig,
        s3,
        zio.connect.s3.live
      )

  lazy val httpClient = NettyHttpClient.default
  lazy val awsConfig  = httpClient >>> AwsConfig.default

  lazy val localStackContainer =
    ZLayer.fromZIO(
      ZIO.acquireRelease(ZIO.attempt {
        val localstackImage = DockerImageName.parse("localstack/localstack:0.11.3")
        val localstack = new LocalStackContainer(localstackImage)
          .withServices(Service.S3)
        localstack.start()
        localstack
      })(ls => ZIO.attempt(ls.stop()).orDie)
    )

  lazy val s3: ZLayer[AwsConfig with LocalStackContainer, Throwable, S3] =
    ZLayer
      .fromZIO(for {
        localstack <- ZIO.service[LocalStackContainer]
        s <- ZIO.succeed(
               S3.customized(
                 _.credentialsProvider(
                   StaticCredentialsProvider
                     .create(AwsBasicCredentials.create(localstack.getAccessKey, localstack.getSecretKey))
                 ).region(Region.of(localstack.getRegion))
                   .endpointOverride(localstack.getEndpointOverride(Service.S3))
               )
             )
      } yield s)
      .flatMap(_.get)

}
