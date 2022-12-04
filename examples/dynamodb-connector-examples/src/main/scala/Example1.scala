import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import zio._
import zio.aws.core.config.AwsConfig
import zio.aws.core.httpclient.HttpClient
import zio.aws.core.{AwsError, GenericAwsError}
import zio.aws.dynamodb.DynamoDb
import zio.aws.dynamodb.model._
import zio.aws.dynamodb.model.primitives._
import zio.aws.netty.NettyHttpClient
import zio.connect.dynamodb._
import zio.stream._

object Example1 extends ZIOAppDefault {
  lazy val httpClient: ZLayer[Any, Throwable, HttpClient] = NettyHttpClient.default
  lazy val awsConfig: ZLayer[Any, Throwable, AwsConfig]   = httpClient >>> AwsConfig.default

  def createTableRequest(tableName: TableName): CreateTableRequest =
    CreateTableRequest(
      tableName = tableName,
      attributeDefinitions = List(
        AttributeDefinition(
          KeySchemaAttributeName("id"),
          ScalarAttributeType.S
        )
      ),
      keySchema = List(
        KeySchemaElement(KeySchemaAttributeName("id"), KeyType.HASH)
      ),
      provisionedThroughput = Some(
        ProvisionedThroughput(
          readCapacityUnits = PositiveLongObject(16L),
          writeCapacityUnits = PositiveLongObject(16L)
        )
      ),
      tableClass = TableClass.STANDARD
    )

  val table = TableName("zio-connect-dynamo-example")

  val key = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("id1")))

  val item = key ++ Map(
    AttributeName("topic")        -> AttributeValue(s = StringAttributeValue("Murakami novels")),
    AttributeName("speechLength") -> AttributeValue(n = NumberAttributeValue("10"))
  )

  val program: ZIO[DynamoDBConnector, AwsError, Unit] =
    for {
      exists <- ZStream(table) >>> tableExists
      _      <- if (exists) ZIO.succeed(()) else ZStream(createTableRequest(table)) >>> createTable
      _ <- (ZStream(PutItemRequest(table, item)) >>> putItem).retryWhile {
             case GenericAwsError(_: ResourceNotFoundException) => true
             case _                                             => false
           }
      response <- ZStream(GetItemRequest(table, key)) >>> getItem
      item      = response.flatMap(_.item.toOption)
      _        <- ZIO.logInfo(item.mkString)
      tables   <- listTables(ListTablesRequest(limit = ListTablesInputLimit(3))).runCollect
      _        <- ZIO.logInfo(tables.mkString(", "))
      _        <- ZStream(DeleteItemRequest(table, key)) >>> deleteItem
      _        <- ZStream(DeleteTableRequest(table)) >>> deleteTable
    } yield ()

  override def run: ZIO[ZIOAppArgs with Scope, Any, Any] =
    program.provide(awsConfig, DynamoDb.live, dynamoDBConnectorLiveLayer)
}
