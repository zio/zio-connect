package zio.connect.dynamodb

import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import zio._
import zio.aws.core.{AwsError, GenericAwsError}
import zio.aws.dynamodb.model._
import zio.aws.dynamodb.model.primitives.{AttributeName, TableName}
import zio.stream.{ZSink, ZStream}

private[dynamodb] trait DynamoDBConnector {

  def batchGetItem(request: => BatchGetItemRequest)(implicit
    trace: Trace
  ): ZStream[Any, AwsError, BatchGetItemResponse]

  def batchWriteItem(request: => BatchWriteItemRequest)(implicit
    trace: Trace
  ): ZStream[Any, AwsError, BatchWriteItemResponse]

  def createTable(implicit trace: Trace): ZSink[Any, AwsError, CreateTableRequest, Nothing, Unit]

  def deleteItem(implicit trace: Trace): ZSink[Any, AwsError, DeleteItemRequest, Nothing, Unit]

  def deleteTable(implicit trace: Trace): ZSink[Any, AwsError, DeleteTableRequest, Nothing, Unit]

  def describeTable(name: => TableName)(implicit trace: Trace): ZStream[Any, AwsError, TableDescription]

  def listTables(request: => ListTablesRequest): ZStream[Any, AwsError, TableName]

  def getItem(request: => GetItemRequest)(implicit trace: Trace): ZStream[Any, AwsError, GetItemResponse]

  def putItem(implicit trace: Trace): ZSink[Any, AwsError, PutItemRequest, Nothing, Unit]

  def query(request: => QueryRequest): ZStream[Any, AwsError, Map[AttributeName, AttributeValue]]

  def scan(request: => ScanRequest): ZStream[Any, AwsError, Map[AttributeName, AttributeValue]]

  def tableExists(implicit trace: Trace): ZSink[Any, AwsError, TableName, TableName, Boolean] =
    ZSink
      .take[TableName](1)
      .map(_.headOption)
      .mapZIO {
        case Some(name) =>
          describeTable(name).runLast.as(true).catchSome { case GenericAwsError(_: ResourceNotFoundException) =>
            ZIO.succeed(false)
          }
        case None => ZIO.succeed(false)
      }

  def updateItem(implicit trace: Trace): ZSink[Any, AwsError, UpdateItemRequest, Nothing, Unit]

  def updateTable(implicit trace: Trace): ZSink[Any, AwsError, UpdateTableRequest, Nothing, Unit]

}
