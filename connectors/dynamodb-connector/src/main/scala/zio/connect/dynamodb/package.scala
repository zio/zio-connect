package zio.connect

import zio.aws.core.AwsError
import zio.aws.dynamodb.DynamoDb
import zio.aws.dynamodb.model._
import zio.aws.dynamodb.model.primitives.{AttributeName, TableName}
import zio.stream.{ZSink, ZStream}
import zio.{Chunk, Trace, ZLayer}

package object dynamodb {

  def batchGetItem(implicit
    trace: Trace
  ): ZSink[DynamoDBConnector, AwsError, BatchGetItemRequest, BatchGetItemRequest, Chunk[BatchGetItemResponse]] =
    ZSink.serviceWithSink[DynamoDBConnector](_.batchGetItem)

  def batchWriteItem(implicit
    trace: Trace
  ): ZSink[DynamoDBConnector, AwsError, BatchWriteItemRequest, BatchWriteItemRequest, Chunk[BatchWriteItemResponse]] =
    ZSink.serviceWithSink[DynamoDBConnector](_.batchWriteItem)

  def createTable(implicit
    trace: Trace
  ): ZSink[DynamoDBConnector, AwsError, CreateTableRequest, CreateTableRequest, Unit] =
    ZSink.serviceWithSink[DynamoDBConnector](_.createTable)

  def deleteItem(implicit trace: Trace): ZSink[DynamoDBConnector, AwsError, DeleteItemRequest, Nothing, Unit] =
    ZSink.serviceWithSink[DynamoDBConnector](_.deleteItem)

  def deleteTable(implicit trace: Trace): ZSink[DynamoDBConnector, AwsError, DeleteTableRequest, Nothing, Unit] =
    ZSink.serviceWithSink[DynamoDBConnector](_.deleteTable)

  def describeTable(implicit
    trace: Trace
  ): ZSink[DynamoDBConnector, AwsError, DescribeTableRequest, DescribeTableRequest, Chunk[DescribeTableResponse]] =
    ZSink.serviceWithSink[DynamoDBConnector](_.describeTable)

  def listTables(request: => ListTablesRequest)(implicit
    trace: Trace
  ): ZStream[DynamoDBConnector, AwsError, TableName] =
    ZStream.serviceWithStream[DynamoDBConnector](_.listTables(request))

  def getItem(implicit
    trace: Trace
  ): ZSink[DynamoDBConnector, AwsError, GetItemRequest, GetItemRequest, Chunk[GetItemResponse]] =
    ZSink.serviceWithSink[DynamoDBConnector](_.getItem)

  def putItem(implicit trace: Trace): ZSink[DynamoDBConnector, AwsError, PutItemRequest, PutItemRequest, Unit] =
    ZSink.serviceWithSink[DynamoDBConnector](_.putItem)

  def query(implicit
    trace: Trace
  ): ZSink[DynamoDBConnector, AwsError, QueryRequest, QueryRequest, Chunk[Map[AttributeName, AttributeValue]]] =
    ZSink.serviceWithSink[DynamoDBConnector](_.query)

  def scan(implicit
    trace: Trace
  ): ZSink[DynamoDBConnector, AwsError, ScanRequest, ScanRequest, Chunk[Map[AttributeName, AttributeValue]]] =
    ZSink.serviceWithSink[DynamoDBConnector](_.scan)

  def tableExists(implicit trace: Trace): ZSink[DynamoDBConnector, AwsError, TableName, TableName, Boolean] =
    ZSink.serviceWithSink[DynamoDBConnector](_.tableExists)

  def updateItem(implicit trace: Trace): ZSink[DynamoDBConnector, AwsError, UpdateItemRequest, Nothing, Unit] =
    ZSink.serviceWithSink[DynamoDBConnector](_.updateItem)

  def updateTable(implicit trace: Trace): ZSink[DynamoDBConnector, AwsError, UpdateTableRequest, Nothing, Unit] =
    ZSink.serviceWithSink[DynamoDBConnector](_.updateTable)

  val dynamoDBConnectorLiveLayer: ZLayer[DynamoDb, Nothing, LiveDynamoDBConnector] = LiveDynamoDBConnector.layer
}
