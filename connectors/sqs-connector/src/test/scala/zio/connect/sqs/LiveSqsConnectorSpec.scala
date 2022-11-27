package zio.connect.sqs

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSClientBuilder}
import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.containers.localstack.LocalStackContainer.Service
import org.testcontainers.utility.DockerImageName
import zio.connect.sqs.SqsConnector.QueueUrl
import zio.test.TestAspect
import zio.{Scope, ZIO, ZLayer}

object LiveSqsConnectorSpec extends SqsConnectorSpec {
  override def spec =
    suite("LiveSqsConnectorSpec")(sqsConnectorSpec)
      .provideSomeShared[Scope](
        localStackContainer,
        sqs,
        queueUrl,
        zio.connect.sqs.sqsConnectorLiveLayer
      ) @@ TestAspect.sequential

  lazy val localStackContainer: ZLayer[Scope, Throwable, LocalStackContainer] =
    ZLayer.fromZIO(
      ZIO.acquireRelease(ZIO.attempt {
        val localstackImage = DockerImageName.parse("localstack/localstack:0.11.3")
        val localstack = new LocalStackContainer(localstackImage)
          .withServices(Service.SQS)
        localstack.start()
        localstack
      })(ls => ZIO.attempt(ls.stop()).orDie)
    )

  lazy val sqs: ZLayer[LocalStackContainer, Throwable, AmazonSQS] =
    ZLayer.fromZIO(for {
      localstack <- ZIO.service[LocalStackContainer]
      awsSqs <- ZIO.attempt {
                  AmazonSQSClientBuilder
                    .standard()
                    .withEndpointConfiguration(
                      new EndpointConfiguration(
                        localstack.getEndpointOverride(Service.SQS).toString,
                        localstack.getRegion
                      )
                    )
                    .withCredentials(new AWSCredentialsProvider {
                      override val getCredentials: AWSCredentials = new AWSCredentials {
                        override def getAWSAccessKeyId: String = localstack.getAccessKey

                        override def getAWSSecretKey: String = localstack.getSecretKey
                      }
                      override def refresh(): Unit = ()
                    })
                    .build()
                }
    } yield awsSqs)

  lazy val queueUrl: ZLayer[AmazonSQS, Throwable, QueueUrl] =
    ZLayer
      .fromZIO(for {
        awsSqs            <- ZIO.service[AmazonSQS]
        queueCreateResult <- ZIO.attempt(awsSqs.createQueue("TestQueue"))
      } yield QueueUrl(queueCreateResult.getQueueUrl))
}
