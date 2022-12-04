package zio.connect.dynamodb

import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import zio._
import zio.aws.core.{AwsError, GenericAwsError}
import zio.aws.dynamodb.model._
import zio.aws.dynamodb.model.primitives.{AttributeName, TableName}
import zio.stream.{ZSink, ZStream}

trait DynamoDBConnector {

  def batchGetItem(implicit
    trace: Trace
  ): ZSink[Any, AwsError, BatchGetItemRequest, BatchGetItemRequest, Chunk[BatchGetItemResponse]]

  def batchWriteItem(implicit
    trace: Trace
  ): ZSink[Any, AwsError, BatchWriteItemRequest, BatchWriteItemRequest, Chunk[BatchWriteItemResponse]]

  def createTable(implicit trace: Trace): ZSink[Any, AwsError, CreateTableRequest, Nothing, Unit]

  def deleteItem(implicit trace: Trace): ZSink[Any, AwsError, DeleteItemRequest, Nothing, Unit]

  def deleteTable(implicit trace: Trace): ZSink[Any, AwsError, DeleteTableRequest, Nothing, Unit]

  def describeTable(implicit
    trace: Trace
  ): ZSink[Any, AwsError, DescribeTableRequest, DescribeTableRequest, Chunk[DescribeTableResponse]]

  def listTables(request: => ListTablesRequest)(implicit trace: Trace): ZStream[Any, AwsError, TableName]

  def getItem(implicit trace: Trace): ZSink[Any, AwsError, GetItemRequest, GetItemRequest, Chunk[GetItemResponse]]

  def putItem(implicit trace: Trace): ZSink[Any, AwsError, PutItemRequest, Nothing, Unit]

  def query(implicit
    trace: Trace
  ): ZSink[Any, AwsError, QueryRequest, QueryRequest, Chunk[Map[AttributeName, AttributeValue]]]

  def scan(implicit
    trace: Trace
  ): ZSink[Any, AwsError, ScanRequest, ScanRequest, Chunk[Map[AttributeName, AttributeValue]]]

  def tableExists(implicit trace: Trace): ZSink[Any, AwsError, TableName, TableName, Boolean] =
    ZSink
      .take[TableName](1)
      .map(_.headOption)
      .mapZIO {
        case Some(name) =>
          (ZStream(DescribeTableRequest(name)) >>> describeTable).as(true).catchSome {
            case GenericAwsError(_: ResourceNotFoundException) =>
              ZIO.succeed(false)
          }
        case None => ZIO.succeed(false)
      }

  def updateItem(implicit trace: Trace): ZSink[Any, AwsError, UpdateItemRequest, Nothing, Unit]

  def updateTable(implicit trace: Trace): ZSink[Any, AwsError, UpdateTableRequest, Nothing, Unit]

}
