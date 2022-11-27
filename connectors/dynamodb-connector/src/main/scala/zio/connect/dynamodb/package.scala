package zio.connect

import zio.aws.core.AwsError
import zio.aws.dynamodb.DynamoDb
import zio.aws.dynamodb.model.primitives.{AttributeName, TableName}
import zio.aws.dynamodb.model._
import zio.stream.{ZPipeline, ZSink, ZStream}
import zio.{RLayer, Trace, ULayer}

package object dynamodb {

  def batchGetItem(request: => BatchGetItemRequest)(implicit
    trace: Trace
  ): ZStream[DynamoDBConnector, AwsError, BatchGetItemResponse] =
    ZStream.serviceWithStream[DynamoDBConnector](_.batchGetItem(request))

  def batchWriteItem(implicit
    trace: Trace
  ): ZPipeline[DynamoDBConnector, AwsError, BatchWriteItemRequest, BatchWriteItemResponse] =
    ZPipeline.serviceWithPipeline[DynamoDBConnector](_.batchWriteItem)

  def createTable(implicit
    trace: Trace
  ): ZSink[DynamoDBConnector, AwsError, CreateTableRequest, CreateTableRequest, Unit] =
    ZSink.serviceWithSink[DynamoDBConnector](_.createTable)

  def deleteItem(implicit trace: Trace): ZSink[DynamoDBConnector, AwsError, DeleteItemRequest, Nothing, Unit] =
    ZSink.serviceWithSink[DynamoDBConnector](_.deleteItem)

  def deleteTable(implicit trace: Trace): ZSink[DynamoDBConnector, AwsError, DeleteTableRequest, Nothing, Unit] =
    ZSink.serviceWithSink[DynamoDBConnector](_.deleteTable)

  def describeTable(request: => TableName)(implicit
    trace: Trace
  ): ZStream[DynamoDBConnector, AwsError, TableDescription] =
    ZStream.serviceWithStream[DynamoDBConnector](_.describeTable(request))

  def listTables(request: => ListTablesRequest)(implicit
    trace: Trace
  ): ZStream[DynamoDBConnector, AwsError, TableName] =
    ZStream.serviceWithStream[DynamoDBConnector](_.listTables(request))

  def getItem(request: => GetItemRequest)(implicit
    trace: Trace
  ): ZStream[DynamoDBConnector, AwsError, GetItemResponse] =
    ZStream.serviceWithStream[DynamoDBConnector](_.getItem(request))

  def putItem(implicit trace: Trace): ZSink[DynamoDBConnector, AwsError, PutItemRequest, PutItemRequest, Unit] =
    ZSink.serviceWithSink[DynamoDBConnector](_.putItem)

  def query(request: => QueryRequest): ZStream[DynamoDBConnector, AwsError, Map[AttributeName, AttributeValue]] =
    ZStream.serviceWithStream[DynamoDBConnector](_.query(request))

  def scan(request: => ScanRequest): ZStream[DynamoDBConnector, AwsError, Map[AttributeName, AttributeValue]] =
    ZStream.serviceWithStream[DynamoDBConnector](_.scan(request))

  def tableExists(implicit trace: Trace): ZSink[DynamoDBConnector, AwsError, TableName, TableName, Boolean] =
    ZSink.serviceWithSink[DynamoDBConnector](_.tableExists)

  def updateItem(implicit trace: Trace): ZSink[DynamoDBConnector, AwsError, UpdateItemRequest, Nothing, Unit] =
    ZSink.serviceWithSink[DynamoDBConnector](_.updateItem)

  def updateTable(implicit trace: Trace): ZSink[DynamoDBConnector, AwsError, UpdateTableRequest, Nothing, Unit] =
    ZSink.serviceWithSink[DynamoDBConnector](_.updateTable)

  val dynamoDBConnectorLiveLayer: RLayer[DynamoDb, LiveDynamoDBConnector] = LiveDynamoDBConnector.layer
  val dynamoDBConnectorTestLayer: ULayer[TestDynamoDBConnector]           = TestDynamoDBConnector.layer
}
