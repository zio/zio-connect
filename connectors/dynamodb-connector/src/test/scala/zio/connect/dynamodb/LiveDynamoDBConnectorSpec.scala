package zio.connect.dynamodb

import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.containers.localstack.LocalStackContainer.Service
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import zio.aws.core.GenericAwsError
import zio.aws.core.config.AwsConfig
import zio.aws.core.httpclient.HttpClient
import zio.aws.dynamodb.DynamoDb
import zio.aws.dynamodb.model._
import zio.aws.dynamodb.model.primitives._
import zio.aws.netty.NettyHttpClient
import zio.prelude._
import zio.stream.ZStream
import zio.test.assertTrue
import zio.{ZIO, ZLayer}

object LiveDynamoDBConnectorSpec extends DynamoDBConnectorSpec {
  lazy val httpClient: ZLayer[Any, Throwable, HttpClient] = NettyHttpClient.default
  lazy val awsConfig: ZLayer[Any, Throwable, AwsConfig]   = httpClient >>> AwsConfig.default

  val liveOnlySpec =
    suite("updateTable")(
      test("successfully updates table") {
        val tableName = TableName("updateTableSpec1")
        val updateTableRequest =
          UpdateTableRequest(tableName = tableName, tableClass = TableClass.STANDARD_INFREQUENT_ACCESS)

        for {
          _ <- ZStream(createTableRequest(tableName)) >>> createTable
          _ <- ZStream(updateTableRequest) >>> updateTable
          tableClass <- (ZStream(DescribeTableRequest(tableName)) >>> describeTable)
                          .map(
                            _.headOption
                              .flatMap(_.table.toOption)
                              .flatMap(_.tableClassSummary.toOption)
                              .flatMap(_.tableClass.toOption)
                          )
        } yield assertTrue(tableClass.get == TableClass.STANDARD_INFREQUENT_ACCESS)
      }
    ) + suite("query")(
      test("successfully queries dynamo db") {
        val tableName     = TableName("query1")
        val item1         = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key1")))
        val item2         = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key2")))
        val keyExpression = KeyExpression("id = :id")
        val expressionAttributeValues = Map(
          ExpressionAttributeValueVariable(":id") -> AttributeValue(s = StringAttributeValue("key1"))
        )
        val queryRequest = QueryRequest(
          tableName,
          keyConditionExpression = keyExpression,
          expressionAttributeValues = expressionAttributeValues
        )

        for {
          _ <- ZStream(createTableRequest(tableName)) >>> createTable
          _ <- ZStream(
                 putItemRequest(tableName, item1),
                 putItemRequest(tableName, item2)
               ) >>> putItem
          response <- ZStream(queryRequest) >>> query
          items     = response.toList.flatMap(getKeyByAttributeName(AttributeName("id")))
        } yield assertTrue(items == List("key1"))
      },
      test("fails if table does not exist") {
        val tableName     = TableName("query2")
        val keyExpression = KeyExpression("id = :id")
        val expressionAttributeValues = Map(
          ExpressionAttributeValueVariable(":id") -> AttributeValue(s = StringAttributeValue("key1"))
        )
        val queryRequest = QueryRequest(
          tableName,
          keyConditionExpression = keyExpression,
          expressionAttributeValues = expressionAttributeValues
        )

        for {
          exit <- (ZStream(queryRequest) >>> query).exit
          failsExpectedly <-
            exit.as(false).catchSome { case GenericAwsError(_: ResourceNotFoundException) => ZIO.succeed(true) }
        } yield assertTrue(failsExpectedly)
      }
    ) + suite("scan")(
      test("successfully scans dynamo db") {
        val tableName   = TableName("scan1")
        val item1       = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key1")))
        val item2       = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key2")))
        val scanRequest = ScanRequest(tableName)

        for {
          _        <- ZStream(createTableRequest(tableName)) >>> createTable
          _        <- ZStream(putItemRequest(tableName, item1), putItemRequest(tableName, item2)) >>> putItem
          response <- ZStream(scanRequest) >>> scan
          items     = response.map(getKeyByAttributeName(AttributeName("id")))
        } yield assertTrue(items.length == 2)
      },
      test("fails if table doesn't exist") {
        val tableName   = TableName("scan2")
        val scanRequest = ScanRequest(tableName)
        for {
          exit <- (ZStream(scanRequest) >>> scan).exit
          failsExpectedly <-
            exit.as(false).catchSome { case GenericAwsError(_: ResourceNotFoundException) => ZIO.succeed(true) }
        } yield assertTrue(failsExpectedly)
      }
    )

  override def spec =
    suite("LiveDynamoDBConnectorSpec")(dynamoDBConnectorSpec + liveOnlySpec)
      .provideShared(awsConfig, localStackContainer, dynamoDb, dynamoDBConnectorLiveLayer)

  lazy val localStackContainer: ZLayer[Any, Throwable, LocalStackContainer] =
    ZLayer.scoped(
      ZIO.acquireRelease(ZIO.attempt {
        val localstackImage = DockerImageName.parse("localstack/localstack:1.2.0")
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
              DynamoDb.customized(_.credentialsProvider(provider).region(Region.US_EAST_1).endpointOverride(endpoint))
            )
      } yield db
    }
}
