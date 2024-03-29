package zio.connect.awslambda

import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.containers.localstack.LocalStackContainer.Service
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import zio.aws.core.config.AwsConfig
import zio.aws.lambda.Lambda
import zio.aws.netty.NettyHttpClient
import zio.{ZIO, ZLayer}

object LiveAwsLambdaConnectorSpec extends AwsLambdaConnectorSpec {

  override def spec =
    suite("LiveAwsLambdaConnectorSpec")(awsLambdaConnectorSpec)
      .provideShared(
        zio.connect.awslambda.awsLambdaConnectorLiveLayer,
        lambda,
        localStackContainer,
        awsConfig
      )

  lazy val httpClient                                   = NettyHttpClient.default
  lazy val awsConfig: ZLayer[Any, Throwable, AwsConfig] = httpClient >>> AwsConfig.default

  lazy val localStackContainer: ZLayer[Any, Throwable, LocalStackContainer] =
    ZLayer.scoped(
      ZIO.acquireRelease(ZIO.attempt {
        val localstackImage = DockerImageName.parse("localstack/localstack:0.13.0")
        val localstack = new LocalStackContainer(localstackImage)
          .withServices(Service.LAMBDA)
        localstack.start()
        localstack
      })(ls => ZIO.attempt(ls.stop()).orDie)
    )

  lazy val lambda: ZLayer[AwsConfig with LocalStackContainer, Throwable, Lambda] =
    ZLayer
      .fromZIO(for {
        localstack <- ZIO.service[LocalStackContainer]
        s <- ZIO.succeed(
               Lambda.customized(
                 _.credentialsProvider(
                   StaticCredentialsProvider
                     .create(AwsBasicCredentials.create(localstack.getAccessKey, localstack.getSecretKey))
                 ).region(Region.of("us-east-2"))
                   .endpointOverride(localstack.getEndpointOverride(Service.LAMBDA))
               )
             )
      } yield s)
      .flatMap(_.get)

}
