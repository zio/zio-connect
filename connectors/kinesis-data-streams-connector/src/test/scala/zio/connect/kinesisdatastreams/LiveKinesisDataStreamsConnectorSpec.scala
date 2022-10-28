package zio.connect.kinesisdatastreams

import nl.vroste.zio.kinesis.client.Producer
import nl.vroste.zio.kinesis.client.serde.Serde
import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.containers.localstack.LocalStackContainer.Service
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import zio.aws.core.config.AwsConfig
import zio.aws.core.httpclient.HttpClient
import zio.aws.netty.NettyHttpClient
import zio.aws.kinesis.Kinesis
import zio.{Scope, ZIO, ZLayer}

object LiveKinesisDataStreamsConnectorSpec extends KinesisDataStreamsConnectorSpec {
  override def spec =
    suite("LiveKinesisDataStreamsConnectorSpec")(kinesisDataStreamsConnectorSpec)
      .provideSomeShared[Scope](
        localStackContainer,
        awsConfig,
        producer,
        zio.connect.kinesisdatastreams.LiveKinesisDataStreamsConnector.layer[String]
      )

  lazy val httpClient: ZLayer[Any, Throwable, HttpClient] = NettyHttpClient.default
  lazy val awsConfig: ZLayer[Any, Throwable, AwsConfig]   = httpClient >>> AwsConfig.default

  lazy val localStackContainer: ZLayer[Scope, Throwable, LocalStackContainer] =
    ZLayer.fromZIO(
      ZIO.acquireRelease(ZIO.attempt {
        val localstackImage = DockerImageName.parse("localstack/localstack:0.11.3")
        val localstack = new LocalStackContainer(localstackImage)
          .withServices(Service.KINESIS)
        localstack.start()
        localstack
      })(ls => ZIO.attempt(ls.stop()).orDie)
    )

  lazy val producer: ZLayer[AwsConfig with LocalStackContainer with Scope, Throwable, Producer[String]] =
    ZLayer
      .fromZIO(for {
        localstack <- ZIO.service[LocalStackContainer]
        p <- Producer
               .make("TestStream", Serde.asciiString)
               .provideSome[Scope](
                 Kinesis.customized(
                   _.credentialsProvider(
                     StaticCredentialsProvider
                       .create(AwsBasicCredentials.create(localstack.getAccessKey, localstack.getSecretKey))
                   ).region(Region.of(localstack.getRegion))
                     .endpointOverride(localstack.getEndpointOverride(Service.KINESIS))
                 ),
                 awsConfig
               )
      } yield p)
}
