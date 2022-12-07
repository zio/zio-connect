package zio.connect.dynamodb

import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.containers.localstack.LocalStackContainer.Service
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import zio.aws.core.config.AwsConfig
import zio.aws.core.httpclient.HttpClient
import zio.aws.dynamodb.DynamoDb
import zio.aws.netty.NettyHttpClient
import zio.{ZIO, ZLayer}

object LiveDynamoDBConnectorSpec extends DynamoDBConnectorSpec {
  lazy val httpClient: ZLayer[Any, Throwable, HttpClient] = NettyHttpClient.default
  lazy val awsConfig: ZLayer[Any, Throwable, AwsConfig]   = httpClient >>> AwsConfig.default

  override def spec =
    suite("LiveDynamoDBConnectorSpec")(dynamoDBConnectorSpec)
      .provideShared(awsConfig, localStackContainer, dynamoDb, dynamoDBConnectorLiveLayer)

  lazy val localStackContainer: ZLayer[Any, Throwable, LocalStackContainer] =
    ZLayer.scoped(
      ZIO.acquireRelease(ZIO.attempt {
        val localstackImage = DockerImageName.parse("localstack/localstack:1.3.0")
        val localstack = new LocalStackContainer(localstackImage)
          .withServices(Service.DYNAMODB)
        localstack.start()
        localstack
      })(container => ZIO.attempt(container.stop()).orDie)
    )

  lazy val dynamoDb: ZLayer[AwsConfig with LocalStackContainer, Throwable, DynamoDb] =
    ZLayer.fromZIO {
      for {
        localstack <- ZIO.service[LocalStackContainer]
        creds       = AwsBasicCredentials.create(localstack.getAccessKey, localstack.getSecretKey)
        provider    = StaticCredentialsProvider.create(creds)
        endpoint    = localstack.getEndpointOverride(Service.DYNAMODB)
        db <-
          ZIO
            .service[DynamoDb]
            .provideSome[AwsConfig](
              DynamoDb.customized(
                _.credentialsProvider(provider).region(Region.of(localstack.getRegion)).endpointOverride(endpoint)
              )
            )
      } yield db
    }
}
