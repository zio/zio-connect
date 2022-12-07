package zio.connect.dynamodb

import software.amazon.awssdk.services.dynamodb.model.{ResourceInUseException, ResourceNotFoundException}
import zio.ZIO
import zio.aws.core.{AwsError, GenericAwsError}
import zio.aws.dynamodb.model._
import zio.aws.dynamodb.model.primitives._
import zio.stream.ZStream
import zio.test.Assertion.equalTo
import zio.test._

import java.util.UUID

trait DynamoDBConnectorSpec extends ZIOSpecDefault {

  protected val dynamoDBConnectorSpec: Spec[DynamoDBConnector, AwsError] =
    batchGetItemSuite + batchWriteItemSuite + itemSuite + listTablesSuite + tableSuite + querySuite + scanSuite

  private lazy val tableSuite = createTableSuite + tableExistsSuite + describeTableSuite + updateTableSuite
  private lazy val itemSuite = putItemSuite + getItemSuite + deleteItemSuite +
    updateItemSuite

  protected def createTableRequest(tableName: TableName): CreateTableRequest =
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

  protected def putItemRequest(tableName: TableName, item: Map[AttributeName, AttributeValue]): PutItemRequest =
    PutItemRequest(tableName = tableName, item = item)

  protected def getItemRequest(tableName: TableName, key: Map[AttributeName, AttributeValue]): GetItemRequest =
    GetItemRequest(tableName = tableName, key = key)

  protected def getKeyByAttributeName(key: AttributeName)(item: Map[AttributeName, AttributeValue]): Option[String] =
    item.get(key).flatMap(_.s.toOption)

  private lazy val createTableSuite = suite("createTable")(
    test("successfully creates table") {
      val tableName = TableName("createTable1")
      for {
        _      <- ZStream(createTableRequest(tableName)) >>> createTable
        exists <- ZStream(tableName) >>> tableExists
      } yield assertTrue(exists)
    },
    test("fails if table already exists") {
      val tableName = TableName("createTable2")
      for {
        _      <- ZStream(createTableRequest(tableName)) >>> createTable
        exists <- ZStream(tableName) >>> tableExists
        exit   <- (ZStream(createTableRequest(tableName)) >>> createTable).exit
        failsWithExpectedError <-
          exit.as(false).catchSome { case GenericAwsError(_: ResourceInUseException) => ZIO.succeed(true) }
      } yield assertTrue(exists) && assertTrue(failsWithExpectedError)
    }
  )

  private lazy val tableExistsSuite = suite("tableExists")(
    test("correctly reports that table exists") {
      val tableName = TableName("tableExists1")
      for {
        _      <- ZStream(createTableRequest(tableName)) >>> createTable
        exists <- ZStream(tableName) >>> tableExists
      } yield assertTrue(exists)
    },
    test("correctly reports table doesn't exist") {
      val tableName = TableName("tableExists2")
      for {
        exists <- ZStream(tableName) >>> tableExists
      } yield assertTrue(!exists)
    }
  )

  private lazy val describeTableSuite =
    suite("describeTable")(
      test("describes a table that exists") {
        val tableName = TableName("describeTable1")
        for {
          _        <- ZStream(createTableRequest(tableName)) >>> createTable
          response <- ZStream(DescribeTableRequest(tableName)) >>> describeTable
          maybeDescription <- ZIO
                                .fromOption(response.headOption)
                                .mapBoth(
                                  _ => AwsError.fromThrowable(new RuntimeException(s"table ${tableName} not found")),
                                  _.table.toOption.flatMap(_.tableName.toOption)
                                )
        } yield assertTrue(maybeDescription.get == tableName)
      },
      test("fails when table does not exist") {
        val tableName = TableName("describeTable2")
        for {
          exit <- (ZStream(DescribeTableRequest(tableName)) >>> describeTable).exit
          failsExpectedly <-
            exit.as(false).catchSome { case GenericAwsError(_: ResourceNotFoundException) => ZIO.succeed(true) }
        } yield assertTrue(failsExpectedly)
      }
    )

  private lazy val putItemSuite =
    suite("putItem")(
      test("successfully puts item") {
        val tableName = TableName("putItem1")
        val item = Map(
          AttributeName("id")      -> AttributeValue(s = StringAttributeValue("key1")),
          AttributeName("column1") -> AttributeValue(n = NumberAttributeValue("10"))
        )
        val request =
          getItemRequest(tableName, Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key1"))))
        for {
          _     <- ZStream(createTableRequest(tableName)) >>> createTable
          _     <- ZStream(putItemRequest(tableName, item)) >>> putItem
          items <- ZStream(request) >>> getItem
          maybeItem <-
            ZIO.fromOption(items.headOption).orElseFail(AwsError.fromThrowable(new RuntimeException(s"item not found")))
        } yield assertTrue(maybeItem.item.isDefined)
      },
      test("fails if table does not exist") {
        val tableName = TableName("putItem2")
        val item      = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key1")))
        for {
          exit <- (ZStream(putItemRequest(tableName, item)) >>> putItem).exit
          failsExpectedly <-
            exit.as(false).catchSome { case GenericAwsError(_: ResourceNotFoundException) => ZIO.succeed(true) }
        } yield assertTrue(failsExpectedly)
      }
    )
  private lazy val getItemSuite = suite("getItem")(
    test("successfully gets an item") {
      val tableName = TableName("getItem1")
      val item1     = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key1")))
      val item2     = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key2")))

      for {
        _     <- ZStream(createTableRequest(tableName)) >>> createTable
        _     <- ZStream(putItemRequest(tableName, item1), putItemRequest(tableName, item2)) >>> putItem
        items <- ZStream(getItemRequest(tableName, item1)) >>> getItem
        maybeItem <-
          ZIO.fromOption(items.headOption).orElseFail(AwsError.fromThrowable(new RuntimeException("item not found")))
        attributeValue = maybeItem.item.toOption.flatMap(_.values.toList.map(_.s.toOption).headOption).flatten
      } yield assertTrue(maybeItem.item.isDefined) && assertTrue(attributeValue.get.contentEquals("key1"))
    },
    test("fails to get item if table doesn't exist") {
      val tableName = TableName("getItem2")
      val item1     = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key1")))

      for {
        exit <- (ZStream(getItemRequest(tableName, item1)) >>> getItem).exit
        failsExpectedly <-
          exit.as(false).catchSome { case GenericAwsError(_: ResourceNotFoundException) => ZIO.succeed(true) }
      } yield assertTrue(failsExpectedly)
    },
    test("returns none if item doesn't exist") {
      val tableName = TableName("getItem3")
      val item1     = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key1")))

      for {
        _     <- ZStream(createTableRequest(tableName)) >>> createTable
        items <- ZStream(getItemRequest(tableName, item1)) >>> getItem
        maybeItem <-
          ZIO.fromOption(items.headOption).orElseFail(AwsError.fromThrowable(new RuntimeException("item not found")))
      } yield assertTrue(maybeItem.item.toOption.get == Map.empty[AttributeName, AttributeValue])
    }
  )
  private lazy val deleteItemSuite = suite("deleteItem")(
    test("successfully deletes an item") {
      val tableName = TableName("deleteItem1")
      val item1     = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key1")))

      for {
        _          <- ZStream(createTableRequest(tableName)) >>> createTable
        _          <- ZStream(putItemRequest(tableName, item1)) >>> putItem
        items      <- ZStream(getItemRequest(tableName, item1)) >>> getItem
        itemWasPut  = items.headOption.flatMap(_.item.toOption).isDefined
        _          <- ZStream(DeleteItemRequest(tableName, item1)) >>> deleteItem
        items1     <- ZStream(getItemRequest(tableName, item1)) >>> getItem
        itemMissing = items1.headOption.flatMap(_.item.toOption)
      } yield assertTrue(itemWasPut && itemMissing.get == Map.empty[AttributeName, AttributeValue])
    },
    test("fails if the table doesn't exist") {
      val item1 = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key1")))

      for {
        exit <- (ZStream(DeleteItemRequest(TableName("tableNotThere"), item1)) >>> deleteItem).exit
        failsExpectedly <-
          exit.as(false).catchSome { case GenericAwsError(_: ResourceNotFoundException) => ZIO.succeed(true) }
      } yield assertTrue(failsExpectedly)
    },
    test("is okay if item doesn't exist initially") {
      val tableName = TableName("deleteItem2")
      val item1     = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key1")))
      val item2     = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key2")))

      for {
        _ <- ZStream(createTableRequest(tableName)) >>> createTable
        _ <- ZStream(putItemRequest(tableName, item2)) >>> putItem // putting in a different item
        exit                 <- (ZStream(DeleteItemRequest(tableName, item1)) >>> deleteItem).exit
        doesntFailExpectedly <- exit.as(true).catchAll(_ => ZIO.succeed(false))
      } yield assertTrue(doesntFailExpectedly)
    }
  )
  private lazy val updateItemSuite = suite("updateItem")(
    test("successfully updates item") {
      val tableName = TableName("updateItem1")
      val item1     = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key1")))
      val updateItemRequest = UpdateItemRequest(
        tableName,
        item1,
        Map(
          AttributeName("authorized") -> AttributeValueUpdate(
            AttributeValue(bool = BooleanAttributeValue(true)),
            AttributeAction.PUT
          )
        )
      )
      for {
        _     <- ZStream(createTableRequest(tableName)) >>> createTable
        _     <- ZStream(putItemRequest(tableName, item1)) >>> putItem
        _     <- ZStream(updateItemRequest) >>> updateItem
        items <- ZStream(getItemRequest(tableName, item1)) >>> getItem
        authorized = items.headOption
                       .flatMap(_.item.toOption)
                       .flatMap(_.get(AttributeName("authorized")))
                       .flatMap(_.bool.toOption)
      } yield assertTrue(authorized.get)
    },
    test("fails if the table doesn't exist") {
      val item1 = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key1")))
      val updateItemRequest = UpdateItemRequest(
        TableName("updateItem2"),
        item1,
        Map(
          AttributeName("authorized") -> AttributeValueUpdate(
            AttributeValue(bool = BooleanAttributeValue(true)),
            AttributeAction.PUT
          )
        )
      )
      for {
        exit <- (ZStream(updateItemRequest) >>> updateItem).exit
        failsExpectedly <-
          exit.as(false).catchSome { case GenericAwsError(_: ResourceNotFoundException) => ZIO.succeed(true) }
      } yield assertTrue(failsExpectedly)
    }
  )

  private lazy val listTablesSuite = suite("listTables")(
    test("successfully lists tables") {
      val tableName = TableName(UUID.randomUUID().toString)
      for {
        initialTables <- listTables(ListTablesRequest()).runCollect
        _             <- ZStream(createTableRequest(tableName)) >>> createTable
        afterTables   <- listTables(ListTablesRequest()).runCollect
      } yield assert(initialTables.contains(tableName))(equalTo(false)) && assertTrue(afterTables.contains(tableName))
    }
  )

  private lazy val batchGetItemSuite =
    suite("batchGetItem")(
      test("successfully gets batched items") {
        val tableName          = TableName("batchGet1")
        val tableName2         = TableName("batchGet2")
        val item1              = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key1")))
        val item2              = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key2")))
        val keysAndAttributes  = KeysAndAttributes(List(item1))
        val keysAndAttributes2 = KeysAndAttributes(List(item2))

        val batchGetItemRequest =
          BatchGetItemRequest(Map(tableName -> keysAndAttributes, tableName2 -> keysAndAttributes2))

        for {
          _        <- ZStream(createTableRequest(tableName), createTableRequest(tableName2)) >>> createTable
          _        <- ZStream(putItemRequest(tableName, item1), putItemRequest(tableName2, item2)) >>> putItem
          response <- ZStream(batchGetItemRequest) >>> batchGetItem
          items     = response.toList.flatMap(_.responses.toOption).flatMap(_.values.flatten)
          keys      = items.flatMap(getKeyByAttributeName(AttributeName("id"))).sorted
        } yield assertTrue(keys == List("key1", "key2"))
      },
      test("fails if table doesn't exist") {
        val tableName           = TableName("batchGet3")
        val item1               = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key1")))
        val item2               = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key2")))
        val keysAndAttributes   = KeysAndAttributes(List(item1, item2))
        val batchGetItemRequest = BatchGetItemRequest(Map(tableName -> keysAndAttributes))

        for {
          exit <- (ZStream(batchGetItemRequest) >>> batchGetItem).exit
          failsExpectedly <-
            exit.as(false).catchSome { case GenericAwsError(_: ResourceNotFoundException) => ZIO.succeed(true) }
        } yield assertTrue(failsExpectedly)
      },
      test("still succeeds if some items are not available") {
        val tableName = TableName("batchGet4")
        val item1     = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key1")))
        val item2     = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key2")))

        val keysAndAttributes = KeysAndAttributes(List(item1, item2))
        val batchGetItemRequest = BatchGetItemRequest(
          Map(tableName -> keysAndAttributes)
        )
        for {
          _        <- ZStream(createTableRequest(tableName)) >>> createTable
          _        <- ZStream(putItemRequest(tableName, item1)) >>> putItem
          response <- (ZStream(batchGetItemRequest) >>> batchGetItem)
          items     = response.toList.flatMap(_.responses.toOption).flatMap(_.values.flatten)
          keys      = items.flatMap(getKeyByAttributeName(AttributeName("id"))).sorted
        } yield assertTrue(keys == List("key1"))
      }
    )

  private lazy val batchWriteItemSuite = suite("batchWriteItem")(
    test("successfully executes batch write request") {
      val tableName = TableName("batchWriteItem1")
      val item1     = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key1")))
      val item2     = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key2")))
      val writeRequests = List(
        WriteRequest(putRequest = PutRequest(item1)),
        WriteRequest(putRequest = PutRequest(item2))
      )
      val batchWriteItemRequest = BatchWriteItemRequest(
        Map(tableName -> writeRequests)
      )

      for {
        _         <- ZStream(createTableRequest(tableName)) >>> createTable
        response  <- ZStream(batchWriteItemRequest) >>> batchWriteItem
        items      = response.flatMap(_.unprocessedItems.toChunk)
        responses <- ZStream(getItemRequest(tableName, item1), getItemRequest(tableName, item2)) >>> getItem
        keys       = responses.flatMap(_.item.toOption).flatMap(getKeyByAttributeName(AttributeName("id"))).toList.sorted
      } yield assertTrue(items.head == Map.empty[TableName, Iterable[WriteRequest]] && keys == List("key1", "key2"))
    },
    test("fails if some tables do not exist") {
      val tableName = TableName("batchWriteItem2")
      val item1     = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key1")))
      val item2     = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key2")))
      val writeRequests = List(
        WriteRequest(putRequest = PutRequest(item1)),
        WriteRequest(putRequest = PutRequest(item2))
      )
      val batchWriteItemRequest = BatchWriteItemRequest(
        Map(tableName -> writeRequests)
      )

      for {
        exit <- (ZStream(batchWriteItemRequest) >>> batchWriteItem).exit
        failsExpectedly <-
          exit.as(false).catchSome { case GenericAwsError(_: ResourceNotFoundException) => ZIO.succeed(true) }
      } yield assertTrue(failsExpectedly)
    }
  )

  private lazy val updateTableSuite = suite("updateTable")(
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
  )

  private lazy val querySuite = suite("query")(
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
  )

  private lazy val scanSuite = suite("scan")(
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
}
