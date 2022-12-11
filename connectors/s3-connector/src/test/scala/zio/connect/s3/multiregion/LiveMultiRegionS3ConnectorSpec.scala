package zio.connect.s3.multiregion

import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.containers.localstack.LocalStackContainer.Service
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import zio.aws.core.config.AwsConfig
import zio.aws.core.httpclient.HttpClient
import zio.aws.netty.NettyHttpClient
import zio.aws.s3.S3
import zio.connect.s3.multiRegionS3ConnectorLiveLayer
import zio.{ZIO, ZLayer}

object LiveMultiRegionS3ConnectorSpec extends MultiRegionS3ConnectorSpec {
  override def spec =
    suite("LiveMultiRegionS3ConnectorSpec")(s3ConnectorSpec)
      .provideShared(
        multiRegionS3ConnectorLiveLayer,
        awsConfig,
        localStackContainer,
        s3
      )

  lazy val httpClient: ZLayer[Any, Throwable, HttpClient] = NettyHttpClient.default
  lazy val awsConfig: ZLayer[Any, Throwable, AwsConfig]   = httpClient >>> AwsConfig.default

  lazy val localStackContainer: ZLayer[Any, Throwable, LocalStackContainer] =
    ZLayer.scoped(
      ZIO.acquireRelease(ZIO.attempt {
        val localstackImage = DockerImageName.parse("localstack/localstack:0.11.3")
        val localstack = new LocalStackContainer(localstackImage)
          .withServices(Service.S3)
        localstack.start()
        localstack
      }.retryN(4))(ls => ZIO.attempt(ls.stop()).orDie)
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
                         ).region(Region.US_WEST_2)
                           .endpointOverride(localstack.getEndpointOverride(Service.S3))
                       )
                     )
      s3USEast2 <- ZIO
                     .service[S3]
                     .provideSome[AwsConfig](
                       S3.customized(
                         _.credentialsProvider(
                           StaticCredentialsProvider
                             .create(AwsBasicCredentials.create(localstack.getAccessKey, localstack.getSecretKey))
                         ).region(Region.US_EAST_2)
                           .endpointOverride(localstack.getEndpointOverride(Service.S3))
                       )
                     )

    } yield Map(Region.US_EAST_2 -> s3USEast2, Region.US_WEST_2 -> s3USWest2)
    ZLayer
      .fromZIO(res)
  }

}
