package zio.connect.dynamodb

import zio.aws.core.AwsError
import zio.aws.dynamodb.DynamoDb
import zio.aws.dynamodb.model._
import zio.aws.dynamodb.model.primitives.{AttributeName, TableName}
import zio.stream.{ZSink, ZStream}
import zio.{Trace, ZIO, ZLayer}

import scala.collection.compat._

case class LiveDynamoDBConnector(db: DynamoDb) extends DynamoDBConnector {
  override def batchGetItem(request: => BatchGetItemRequest)(implicit
    trace: Trace
  ): ZStream[Any, AwsError, BatchGetItemResponse] =
    ZStream.fromZIO(db.batchGetItem(request).map(_.asEditable))

  override def batchWriteItem(request: => BatchWriteItemRequest)(implicit
    trace: Trace
  ): ZStream[Any, AwsError, BatchWriteItemResponse] =
    ZStream.fromZIO(db.batchWriteItem(request)).map(_.asEditable)

  override def createTable(implicit trace: Trace): ZSink[Any, AwsError, CreateTableRequest, Nothing, Unit] =
    ZSink.foreach[Any, AwsError, CreateTableRequest](db.createTable)

  override def deleteItem(implicit trace: Trace): ZSink[Any, AwsError, DeleteItemRequest, Nothing, Unit] =
    ZSink.foreach[Any, AwsError, DeleteItemRequest](db.deleteItem)

  override def deleteTable(implicit trace: Trace): ZSink[Any, AwsError, DeleteTableRequest, Nothing, Unit] =
    ZSink.foreach[Any, AwsError, DeleteTableRequest](db.deleteTable)

  override def describeTable(name: => TableName)(implicit trace: Trace): ZStream[Any, AwsError, TableDescription] = {
    val request = DescribeTableRequest(name)
    ZStream.fromZIO(db.describeTable(request).flatMap(_.getTable.map(_.asEditable)))
  }
  override def getItem(request: => GetItemRequest)(implicit trace: Trace): ZStream[Any, AwsError, GetItemResponse] =
    ZStream.fromZIO(db.getItem(request).map(_.asEditable))

  override def listTables(request: => ListTablesRequest): ZStream[Any, AwsError, TableName] = db.listTables(request)

  override def putItem(implicit trace: Trace): ZSink[Any, AwsError, PutItemRequest, Nothing, Unit] =
    ZSink.foreach[Any, AwsError, PutItemRequest](db.putItem)

  override def updateItem(implicit trace: Trace): ZSink[Any, AwsError, UpdateItemRequest, Nothing, Unit] =
    ZSink.foreach[Any, AwsError, UpdateItemRequest](db.updateItem)

  override def updateTable(implicit trace: Trace): ZSink[Any, AwsError, UpdateTableRequest, Nothing, Unit] =
    ZSink.foreach[Any, AwsError, UpdateTableRequest](db.updateTable)

  override def query(request: => QueryRequest): ZStream[Any, AwsError, Map[AttributeName, AttributeValue]] =
    db.query(request).map(_.view.mapValues(_.asEditable).toMap)

  override def scan(request: => ScanRequest): ZStream[Any, AwsError, Map[AttributeName, AttributeValue]] =
    db.scan(request).map(_.view.mapValues(_.asEditable).toMap)
}

object LiveDynamoDBConnector {
  val layer: ZLayer[DynamoDb, Nothing, LiveDynamoDBConnector] =
    ZLayer.fromZIO(ZIO.service[DynamoDb].map(LiveDynamoDBConnector(_)))
}
