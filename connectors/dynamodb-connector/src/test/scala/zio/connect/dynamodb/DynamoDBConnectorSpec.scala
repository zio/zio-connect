package zio.connect.dynamodb

import software.amazon.awssdk.services.dynamodb.model.{ResourceInUseException, ResourceNotFoundException}
import zio.ZIO
import zio.aws.core.{AwsError, GenericAwsError}
import zio.aws.dynamodb.model._
import zio.aws.dynamodb.model.primitives.{
  AttributeName,
  KeySchemaAttributeName,
  NumberAttributeValue,
  PositiveLongObject,
  StringAttributeValue,
  TableName
}
import zio.stream.ZStream
import zio.test._

trait DynamoDBConnectorSpec extends ZIOSpecDefault {

  protected val dynamoDBConnectorSpec: Spec[DynamoDBConnector, AwsError] = tableSuite + itemSuite

  private def createTableRequest(tableName: TableName) =
    CreateTableRequest(
      tableName = tableName,
      attributeDefinitions = List(
        AttributeDefinition(
          KeySchemaAttributeName("key"),
          ScalarAttributeType.S
        )
      ),
      keySchema = List(
        KeySchemaElement(KeySchemaAttributeName("key"), KeyType.HASH)
      ),
      provisionedThroughput = Some(
        ProvisionedThroughput(
          readCapacityUnits = PositiveLongObject(16L),
          writeCapacityUnits = PositiveLongObject(16L)
        )
      )
    )

  private def putItemRequest(tableName: TableName, item: Map[AttributeName, AttributeValue]) =
    PutItemRequest(tableName = tableName, item = item)

  private def getItemRequest(tableName: TableName, key: Map[AttributeName, AttributeValue]) =
    GetItemRequest(tableName = tableName, key = key)

  private lazy val tableSuite: Spec[DynamoDBConnector, AwsError] =
    suite("createTable")(
      test("successfully creates table") {
        val spec1 = TableName("spec1")
        for {
          _      <- ZStream(createTableRequest(spec1)) >>> createTable
          exists <- ZStream(spec1) >>> tableExists
        } yield assertTrue(exists)
      },
      test("fails if table already exists") {
        val spec2 = TableName("spec2")
        for {
          _      <- ZStream(createTableRequest(spec2)) >>> createTable
          exists <- ZStream(spec2) >>> tableExists
          exit   <- (ZStream(createTableRequest(spec2)) >>> createTable).exit
          failsWithExpectedError <-
            exit.as(false).catchSome { case GenericAwsError(_: ResourceInUseException) => ZIO.succeed(true) }
        } yield assertTrue(exists) && assertTrue(failsWithExpectedError)
      }
    ) +
      suite("tableExists")(
        test("correctly reports that table exists") {
          val spec3 = TableName("spec3")
          for {
            _      <- ZStream(createTableRequest(spec3)) >>> createTable
            exists <- ZStream(spec3) >>> tableExists
          } yield assertTrue(exists)
        },
        test("correctly reports table doesn't exist") {
          val table = TableName("notthere")
          for {
            exists <- ZStream(table) >>> tableExists
          } yield assertTrue(!exists)
        }
      ) +
      suite("describeTable")(
        test("describes a table that exists") {
          val spec4 = TableName("spec4")
          for {
            _              <- ZStream(createTableRequest(spec4)) >>> createTable
            maybeDescribed <- describeTable(spec4).runHead
            tableName <- ZIO
                           .fromOption(maybeDescribed)
                           .mapBoth(
                             _ => AwsError.fromThrowable(new RuntimeException(s"table ${spec4} not found")),
                             _.tableName.toOption
                           )
          } yield assertTrue(tableName.get == spec4)
        },
        test("fails when table does not exist") {
          val spec5 = TableName("spec5")
          for {
            exit <- describeTable(spec5).runHead.exit
            failsExpectedly <-
              exit.as(false).catchSome { case GenericAwsError(_: ResourceNotFoundException) => ZIO.succeed(true) }
          } yield assertTrue(failsExpectedly)
        }
      )

  private lazy val itemSuite: Spec[DynamoDBConnector, AwsError] =
    suite("putItem")(
      test("successfully puts item") {
        val spec6 = TableName("spec6")
        val item: Map[AttributeName, AttributeValue] = Map(
          AttributeName("key")     -> AttributeValue(s = StringAttributeValue("key1")),
          AttributeName("column1") -> AttributeValue(n = NumberAttributeValue("10"))
        )
        for {
          _ <- ZStream(createTableRequest(spec6)) >>> createTable
          _ <- ZStream(putItemRequest(spec6, item)) >>> putItem
          maybeItem <- getItem(
                         getItemRequest(
                           spec6,
                           Map(AttributeName("key") -> AttributeValue(s = StringAttributeValue("key1")))
                         )
                       ).runHead
          maybeItem0 <-
            ZIO.fromOption(maybeItem).orElseFail(AwsError.fromThrowable(new RuntimeException(s"item not found")))
        } yield assertTrue(maybeItem0.item.isDefined)
      },
      test("fails if table does not exist") {
        val spec7 = TableName("spec7")
        val item: Map[AttributeName, AttributeValue] = Map(
          AttributeName("key") -> AttributeValue(s = StringAttributeValue("key1"))
        )
        for {
          exit <- (ZStream(putItemRequest(spec7, item)) >>> putItem).exit
          failsExpectedly <-
            exit.as(false).catchSome { case GenericAwsError(_: ResourceNotFoundException) => ZIO.succeed(true) }
        } yield assertTrue(failsExpectedly)
      }
    ) + suite("getItem")(
      test("successfully gets an item") {
        val spec8 = TableName("spec8")
        val item1: Map[AttributeName, AttributeValue] = Map(
          AttributeName("key") -> AttributeValue(s = StringAttributeValue("key1"))
        )
        val item2: Map[AttributeName, AttributeValue] = Map(
          AttributeName("key") -> AttributeValue(s = StringAttributeValue("key2"))
        )
        for {
          _         <- ZStream(createTableRequest(spec8)) >>> createTable
          _         <- ZStream(putItemRequest(spec8, item1), putItemRequest(spec8, item2)) >>> putItem
          maybeItem <- getItem(getItemRequest(spec8, item1)).runHead
          maybeItem0 <-
            ZIO.fromOption(maybeItem).orElseFail(AwsError.fromThrowable(new RuntimeException("item not found")))
          attributeValue = maybeItem0.item.toOption.flatMap(_.values.toList.map(_.s.toOption).headOption).flatten
        } yield assertTrue(maybeItem0.item.isDefined) && assertTrue(attributeValue.get.contentEquals("key1"))
      },
      test("fails to get item if table doesn't exist") {
        val spec9 = TableName("spec9")
        val item1: Map[AttributeName, AttributeValue] = Map(
          AttributeName("key") -> AttributeValue(s = StringAttributeValue("key1"))
        )
        for {
          exit <- getItem(getItemRequest(spec9, item1)).runHead.exit
          failsExpectedly <-
            exit.as(false).catchSome { case GenericAwsError(_: ResourceNotFoundException) => ZIO.succeed(true) }
        } yield assertTrue(failsExpectedly)
      },
      test("returns none if item doesn't exist") {
        val spec10 = TableName("spec10")
        val item1: Map[AttributeName, AttributeValue] = Map(
          AttributeName("key") -> AttributeValue(s = StringAttributeValue("key1"))
        )
        for {
          _         <- ZStream(createTableRequest(spec10)) >>> createTable
          maybeItem <- getItem(getItemRequest(spec10, item1)).runHead
          maybeItem0 <-
            ZIO.fromOption(maybeItem).orElseFail(AwsError.fromThrowable(new RuntimeException("item not found")))
        } yield assertTrue(maybeItem0.item.toOption.get == Map.empty[AttributeName, AttributeValue])
      }
    )

}
