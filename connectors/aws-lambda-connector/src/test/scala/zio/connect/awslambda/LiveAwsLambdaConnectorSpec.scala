package zio.connect.awslambda

import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.containers.localstack.LocalStackContainer.Service
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import zio.aws.core.config.AwsConfig
import zio.aws.lambda.Lambda
import zio.aws.netty.NettyHttpClient
import zio.{Scope, ZIO, ZLayer}

object LiveAwsLambdaConnectorSpec extends AwsLambdaConnectorSpec {

  override def spec =
    suite("LiveAwsLambdaConnectorSpec")(awsLambdaConnectorSpec)
      .provideSomeShared[Scope with LocalStackContainer with Lambda with AwsConfig](
        zio.connect.awslambda.awsLambdaConnectorLiveLayer
      )
      .provideSomeLayerShared[Scope with AwsConfig with LocalStackContainer](
        lambda
      )
      .provideSomeLayerShared[Scope with AwsConfig](
        localStackContainer
      )
      .provideSomeLayerShared[Scope](
        awsConfig
      )

  lazy val httpClient                                   = NettyHttpClient.default
  lazy val awsConfig: ZLayer[Any, Throwable, AwsConfig] = httpClient >>> AwsConfig.default

  lazy val localStackContainer: ZLayer[Scope, Throwable, LocalStackContainer] =
    ZLayer.fromZIO(
      ZIO.acquireRelease(ZIO.attempt {
        val localstackImage = DockerImageName.parse("localstack/localstack:0.13.0")
        val localstack = new LocalStackContainer(localstackImage)
          .withServices(Service.LAMBDA, Service.API_GATEWAY)
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
