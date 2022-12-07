package zio.connect.dynamodb

import zio.aws.core.AwsError
import zio.aws.dynamodb.DynamoDb
import zio.aws.dynamodb.model._
import zio.aws.dynamodb.model.primitives.{AttributeName, TableName}
import zio.stream.{ZSink, ZStream}
import zio.{Chunk, Trace, ZIO, ZLayer}

import scala.collection.compat._

case class LiveDynamoDBConnector(db: DynamoDb) extends DynamoDBConnector {
  override def batchGetItem(implicit
    trace: Trace
  ): ZSink[Any, AwsError, BatchGetItemRequest, BatchGetItemRequest, Chunk[BatchGetItemResponse]] =
    ZSink.foldLeftZIO(Chunk.empty[BatchGetItemResponse]) { case (chunk, request) =>
      db.batchGetItem(request).map(_.asEditable).map(chunk :+ _)
    }

  override def batchWriteItem(implicit
    trace: Trace
  ): ZSink[Any, AwsError, BatchWriteItemRequest, BatchWriteItemRequest, Chunk[BatchWriteItemResponse]] =
    ZSink.foldLeftZIO(Chunk.empty[BatchWriteItemResponse]) { case (chunk, request) =>
      db.batchWriteItem(request).map(_.asEditable).map(chunk :+ _)
    }

  override def createTable(implicit trace: Trace): ZSink[Any, AwsError, CreateTableRequest, Nothing, Unit] =
    ZSink.foreach[Any, AwsError, CreateTableRequest](db.createTable)

  override def deleteItem(implicit trace: Trace): ZSink[Any, AwsError, DeleteItemRequest, Nothing, Unit] =
    ZSink.foreach[Any, AwsError, DeleteItemRequest](db.deleteItem)

  override def deleteTable(implicit trace: Trace): ZSink[Any, AwsError, DeleteTableRequest, Nothing, Unit] =
    ZSink.foreach[Any, AwsError, DeleteTableRequest](db.deleteTable)

  override def describeTable(implicit
    trace: Trace
  ): ZSink[Any, AwsError, DescribeTableRequest, DescribeTableRequest, Chunk[DescribeTableResponse]] =
    ZSink.foldLeftZIO(Chunk.empty[DescribeTableResponse]) { case (chunk, request) =>
      db.describeTable(request).map(_.asEditable).map(chunk :+ _)
    }
  override def getItem(implicit
    trace: Trace
  ): ZSink[Any, AwsError, GetItemRequest, GetItemRequest, Chunk[GetItemResponse]] =
    ZSink.foldLeftZIO(Chunk.empty[GetItemResponse]) { case (chunk, request) =>
      db.getItem(request).map(_.asEditable).map(chunk :+ _)
    }

  override def listTables(request: => ListTablesRequest)(implicit trace: Trace): ZStream[Any, AwsError, TableName] =
    db.listTables(request)

  override def putItem(implicit trace: Trace): ZSink[Any, AwsError, PutItemRequest, Nothing, Unit] =
    ZSink.foreach[Any, AwsError, PutItemRequest](db.putItem)

  override def query(implicit
    trace: Trace
  ): ZSink[Any, AwsError, QueryRequest, QueryRequest, Chunk[Map[AttributeName, AttributeValue]]] =
    ZSink.foldLeftZIO(Chunk.empty[Map[AttributeName, AttributeValue]]) { case (chunk, request) =>
      db.query(request).map(_.view.mapValues(_.asEditable).toMap).runCollect.map(chunk ++ _)
    }

  override def scan(implicit
    trace: Trace
  ): ZSink[Any, AwsError, ScanRequest, ScanRequest, Chunk[Map[AttributeName, AttributeValue]]] =
    ZSink.foldLeftZIO(Chunk.empty[Map[AttributeName, AttributeValue]]) { case (chunk, request) =>
      db.scan(request).map(_.view.mapValues(_.asEditable).toMap).runCollect.map(chunk ++ _)
    }

  override def updateItem(implicit trace: Trace): ZSink[Any, AwsError, UpdateItemRequest, Nothing, Unit] =
    ZSink.foreach[Any, AwsError, UpdateItemRequest](db.updateItem)

  override def updateTable(implicit trace: Trace): ZSink[Any, AwsError, UpdateTableRequest, Nothing, Unit] =
    ZSink.foreach[Any, AwsError, UpdateTableRequest](db.updateTable)
}

object LiveDynamoDBConnector {
  val layer: ZLayer[DynamoDb, Nothing, LiveDynamoDBConnector] =
    ZLayer.fromZIO(ZIO.service[DynamoDb].map(LiveDynamoDBConnector(_)))
}
