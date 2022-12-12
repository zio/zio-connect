---
id: dynamodb-connector
title: "DynamoDB Connector"
---

Setup
-----

```scala
libraryDependencies += "dev.zio" %% "zio-connect-dynamodb" % "@VERSION@"
```

How to use it?

All available DynamoDBConnector combinators and operations are available in the package object `zio.connect.dynamodb._`
you will need to import that to get started.

Additionally, you must also configure and provide the underlying `DynamoDB` layer provided by `zio-aws` 
you can read more about how to configure it [here][zio-aws]

If you have default credentials in the system environment typically at `~/.aws/credentials` or as env variables
the following configuration will likely work.

[zio-aws]: https://zio.github.io/zio-aws/docs/overview/overview_config

```scala
import zio._
import zio.aws.netty.NettyHttpClient
import zio.aws.core.config.AwsConfig
import zio.aws.core.httpclient.HttpClient
import zio.connect.dynamodb._

lazy val httpClient: ZLayer[Any, Throwable, HttpClient] = NettyHttpClient.default
lazy val awsConfig: ZLayer[Any, Throwable, AwsConfig]   = httpClient >>> AwsConfig.default
```

Almost everything in this api requires the existence of a table, we utilize the models provided by `zio-aws`
to create tables, requests, responses, and all other DynamoDB related types. These are typically modeled as 
`new-types` from zio-prelude or case classes.

Here's a create table request:

```scala
import zio.aws.dynamodb.model._
import zio.aws.dynamodb.model.primitives._

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
```

DynamoDB is "schemaless" in the sense that put data of different shapes in different rows, but you must define
a schema for the keys that the table depends on, these are called "partition keys" and "sort keys" or "hash" and "range" keys.
Only a partition/hash key is required, and you should only define attributes for the fields which constitute your 
`keySchema`. A valid table definition must also declare a `provisionedThroughput`, but there are _many_ other
options available for creating tables, you can read more about tables [here][dynamodb-tables].

[dynamodb-tables]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.CoreComponents.html#HowItWorks.CoreComponents.Tables

Once you have a table definition you can create a table using the `createTable` combinator:

```scala
val tableName = TableName("my-table")
val createTableAction: ZIO[DynamoDBConnector, AwsError, CreateTableResponse] = createTable(createTableRequest(tableName)) >>> createTable
```

Dynamo tables can take a moment to be created, so you'll want to have some kind of retry mechanism when performing
subsequent operations on the table. To illustrate, let's put an item into the table:

```scala
val putItemAction: ZIO[DynamoDBConnector, AwsError, PutItemResponse] = 
  ZStream(PutItemRequest(tableName, Map("id" -> AttributeValue(s = StringAttributeValue("my-id"))))) >>> putItem

val putItemWithRetry: ZIO[DynamoDBConnector, AwsError, PutItemResponse] =
    putItemAction.retryWhile {
      case GenericAwsError(_: ResourceNotFoundException) => true
      case _ => false
    }
```

Let's say we want to get an item from the table, we can use the `getItem` combinator, note we need to
provide the _full_ key here to use it:

```scala
val key = Map("id" -> AttributeValue(s = StringAttributeValue("my-id")))
val getItemAction: ZIO[DynamoDBConnector, AwsError, GetItemResponse] = 
  getItem(GetItemRequest(tableName, key)) >>> getItem
```
Currently, entries from the table are returned as a `Map[AttributeName, AttributeValue]` this may change in the future.

And we can also delete an item from the table:

```scala
val deleteItemAction: ZIO[DynamoDBConnector, AwsError, DeleteItemResponse] =
deleteItem(DeleteItemRequest(tableName, key)) >>> deleteItem
```

In order to run a program involving the DynamoDBConnector you need to provide a live `DynamoDB` from `zio-aws` along with config
and the live connector layer:

```scala
override def run: ZIO[ZIOAppArgs with Scope, Any, Any] =
     program.provide(awsConfig, DynamoDb.live, dynamoDBConnectorLiveLayer)
```

`dynamoDBConnectorLiveLayer` is a `ZLayer` that provides the `LiveDynamoDBConnector`

Operators
---

The following operations are available:

## `batchGetItem`

Accepts a stream of `BatchGetItemRequest` and returns a `Chunk` of `BatchGetItemResponse`, if 
one of the tables in the request is not available, the entire request will fail. 
The response object will contain and `unprocessedKeys` field which can be used to retry the request.

```scala
val item1              = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key1")))
val item2              = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key2")))
val keysAndAttributes  = KeysAndAttributes(List(item1))
val keysAndAttributes2 = KeysAndAttributes(List(item2))

val batchGetItemRequest =
  BatchGetItemRequest(Map(tableName -> keysAndAttributes, tableName2 -> keysAndAttributes2))
  
val batchGetItemAction: ZIO[DynamoDBConnector, AwsError, Chunk[BatchGetItemResponse]] =
  batchGetItem(ZStream(batchGetItemRequest)) >>> batchGetItem
```          

## `batchWriteItem`

Accepts a stream of `BatchWriteItemRequest` and returns a `Chunk` of `BatchWriteItemResponse`, if
one of the tables in the request is not available, the entire request will fail. This is used to 
simultaneously put and delete items on multiple tables, you cannot write and delete an item
with the same key in the same table in a single request. The response object does
have an `unprocessedKeys` field which can be used to retry the remaining requests.

```scala
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

val batchWriteItemAction: ZIO[DynamoDBConnector, AwsError, Chunk[BatchWriteItemResponse]] =
  batchWriteItem(ZStream(batchWriteItemRequest)) >>> batchWriteItem
```

## `createTable`

Accepts a stream of `CreateTableRequest` and returns `Unit`. Will fail with a `ResourceInUseException` if the table already exists.

```scala
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
    
val createTableAction: ZIO[DynamoDBConnector, AwsError, Unit] =
    ZStream(createTableRequest(tableName)) >>> createTable
```

## `deleteItem`

Accepts a stream of `DeleteItemRequest` and returns `Unit`. Will fail with a 
`ResourceNotFoundException` if the table does not exist, does 
not fail if the item does not exist.

```scala
val item1 = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key1")))
val deleteItemAction: ZIO[DynamoDBConnector, AwsError, Unit] = ZStream(DeleteItemRequest(tableName, item1)) >>> deleteItem
```

## `deleteTable`

Accepts a stream of `DeleteTableRequest` and returns `Unit`. Will fail
with `ResourceNotFoundException` if the table does not exist.

```scala
val deleteTableAction: ZIO[DynamoDBConnector, AwsError, Unit] = ZStream(DeleteTableRequest(tableName)) >>> deleteTable
```

## `describeTable`

Accepts a stream of `DescribeTableRequest` and returns a `Chunk` of `DescribeTableResponse`. Will fail
if the table does not exist.

```scala
val describeTableAction: ZIO[DynamoDBConnector, AwsError, Chunk[DescribeTableResponse]] = ZStream(DescribeTableRequest(tableName)) >>> describeTable
```

## `listTables`

Takes a `ListTableRequest` and return a Stream of `TableName`, can also provide a limit to the number of tables returned.

```scala
val listTablesAction: ZIO[DynamoDBConnector, AwsError, TableName] = listTables(ListTablesRequest()).runCollect
```

## `getItem`

Accepts a stream of `GetItemRequest` and returns a `Chunk` of `GetItemResponse`. 
Will fail with a `ResourceNotFoundException` if the table does not exist.

```scala
val item1 = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key1")))
val getItemAction: ZIO[DynamoDBConnector, AwsError, Chunk[GetItemResponse]] = ZStream(GetItemRequest(tableName, item1)) >>> getItem
```

## `putItem`

Accepts a stream of `PutItemRequest` and returns `Unit`. Will fail with a `ResourceNotFoundException` if the table does not exist.

```scala
val item1 = Map(AttributeName("id") -> AttributeValue(s = StringAttributeValue("key1")))
val putItemAction: ZIO[DynamoDBConnector, AwsError, Unit] = ZStream(PutItemRequest(tableName, item1)) >>> putItem
```

## `query`

Accepts a stream of `QueryRequest` and returns a `Chunk` of `Map[AttributeName, AttributeValue]`. 
Will fail with a `ResourceNotFoundException` if the table does not exist.

```scala
val tableName     = TableName("query1")
val keyExpression = KeyExpression("id = :id")
val expressionAttributeValues = Map(
  ExpressionAttributeValueVariable(":id") -> AttributeValue(s = StringAttributeValue("key1"))
)
val queryRequest = QueryRequest(
  tableName,
  keyConditionExpression = keyExpression,
  expressionAttributeValues = expressionAttributeValues
)

val queryAction: ZIO[DynamoDBConnector, AwsError, Chunk[Map[AttributeName, AttributeValue]]] =
  ZStream(queryRequest) >>> query
```

## `scan`

Similar to query, but if you don't know the key, you can use scan to return some items in a table or filter
by some non-key condition. Accepts a stream of `ScanRequest` returns a `Chunk` of `Map[AttributeName, AttributeValue]`.
Fails if the table does not exist.

```scala
val tableName     = TableName("scan1")
val scanRequest = ScanRequest(tableName)

val scanAction: ZIO[DynamoDBConnector, AwsError, Chunk[Map[AttributeName, AttributeValue]]] =
  ZStream(scanRequest) >>> scan
```

## `tableExists`

Given a `TableName`, returns a `Boolean` indicating if the table exists.

```scala
val tableExistsAction: ZIO[DynamoDBConnector, AwsError, Boolean] = ZStream(tableName) >>> tableExists
```

## `updateItem`

Accepts a stream of `UpdateItemRequest` and returns `Unit`. Will fail with a `ResourceNotFoundException` if the table does not exist.

```scala
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

val updateItemAction: ZIO[DynamoDBConnector, AwsError, Unit] =
  ZStream(updateItemRequest) >>> updateItem
```

## `updateTable`

Accepts a stream of `UpdateTableRequest` and returns `Unit`. Will fail with a `ResourceNotFoundException` if the table does not exist.

```scala
val tableName = TableName("updateTable1")
val updateTableRequest = UpdateTableRequest(
  tableName,
  provisionedThroughput = Some(
    ProvisionedThroughput(
      readCapacityUnits = PositiveLongObject(16L),
      writeCapacityUnits = PositiveLongObject(16L)
    )
  )
)

val updateTableAction: ZIO[DynamoDBConnector, AwsError, Unit] =
  ZStream(updateTableRequest) >>> updateTable
```
