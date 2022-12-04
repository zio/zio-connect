package zio.connect.dynamodb

import software.amazon.awssdk.services.dynamodb.model.{ResourceInUseException, ResourceNotFoundException}
import zio.aws.core.AwsError
import zio.aws.dynamodb.model._
import zio.aws.dynamodb.model.primitives.{AttributeName, TableName}
import zio.connect.dynamodb.TestDynamoDBConnector._
import zio.stm.{STM, TRef, ZSTM}
import zio.stream.{ZSink, ZStream}
import zio.{Chunk, Trace, ULayer, ZIO, ZLayer}

import scala.collection.compat._

private[dynamodb] final case class TestDynamoDBConnector(db: TestDynamoDb) extends DynamoDBConnector {
  override def batchGetItem(request: => BatchGetItemRequest)(implicit
    trace: Trace
  ): ZStream[Any, AwsError, BatchGetItemResponse] =
    ZStream.fromZIO(db.batchGetItem(request))

  override def batchWriteItem(request: => BatchWriteItemRequest)(implicit
    trace: Trace
  ): ZStream[Any, AwsError, BatchWriteItemResponse] =
    ZStream.fromZIO(db.batchWriteItem(request)).flattenIterables

  override def createTable(implicit trace: Trace): ZSink[Any, AwsError, CreateTableRequest, Nothing, Unit] =
    ZSink.foreach(db.createTable)

  override def deleteItem(implicit trace: Trace): ZSink[Any, AwsError, DeleteItemRequest, Nothing, Unit] =
    ZSink.foreach[Any, AwsError, DeleteItemRequest](db.deleteItem)

  override def deleteTable(implicit trace: Trace): ZSink[Any, AwsError, DeleteTableRequest, Nothing, Unit] =
    ZSink.foreach[Any, AwsError, DeleteTableRequest](db.deleteTable)

  override def describeTable(name: => TableName)(implicit trace: Trace): ZStream[Any, AwsError, TableDescription] =
    ZStream.fromZIO(db.describeTable(DescribeTableRequest(name)))

  override def getItem(request: => GetItemRequest)(implicit trace: Trace): ZStream[Any, AwsError, GetItemResponse] =
    ZStream.fromZIO(db.getItem(request))

  override def listTables(request: => ListTablesRequest): ZStream[Any, AwsError, TableName] =
    ZStream.fromZIO(db.listTables(request)).flattenIterables

  override def putItem(implicit trace: Trace): ZSink[Any, AwsError, PutItemRequest, Nothing, Unit] =
    ZSink.foreach[Any, AwsError, PutItemRequest](db.putItem)

  override def query(request: => QueryRequest): ZStream[Any, AwsError, Map[AttributeName, AttributeValue]] =
    ZStream.fromZIO(db.query(request)).flattenIterables

  override def scan(request: => ScanRequest): ZStream[Any, AwsError, Map[AttributeName, AttributeValue]] =
    ZStream.fromZIO(db.scan(request)).flattenIterables

  override def updateItem(implicit trace: Trace): ZSink[Any, AwsError, UpdateItemRequest, Nothing, Unit] =
    ZSink.foreach[Any, AwsError, UpdateItemRequest](db.updateItem)

  override def updateTable(implicit trace: Trace): ZSink[Any, AwsError, UpdateTableRequest, Nothing, Unit] =
    ZSink.foreach[Any, AwsError, UpdateTableRequest](db.updateTable)
}

object TestDynamoDBConnector {

  type DB = Map[TableName, Option[Chunk[Map[AttributeName, AttributeValue]]]]

  private[dynamodb] final case class TestDynamoDb(store: TRef[DB]) {
    def batchGetItem(request: BatchGetItemRequest): ZIO[Any, AwsError, BatchGetItemResponse] =
      ZSTM.atomically {
        val requestedTables = request.requestItems.keys.toList
        for {
          allTablesExist <- store.get.map(store0 => requestedTables.forall(store0.keys.toList.contains))
          items <- if (allTablesExist) {
                     ZSTM
                       .foreach(request.requestItems.toList) { case (table, requests) =>
                         ZSTM
                           .foreach(requests.keys.toList) { key =>
                             store.get.map(_.fetchItem(table, key))
                           }
                           .map(res => table -> res.flatten)
                       }
                       .map(_.toMap)
                       .map(responses => BatchGetItemResponse(responses))

                   } else resourceNotFound(s"One or more of ${requestedTables.mkString(", ")} was not found")
        } yield items
      }

    def batchWriteItem(request: BatchWriteItemRequest): ZIO[Any, AwsError, List[BatchWriteItemResponse]] =
      ZSTM.atomically {
        val requestedTables = request.requestItems.keys.toList
        for {
          allTablesExist <- store.get.map(store0 => requestedTables.forall(store0.keys.toList.contains))
          res <- if (allTablesExist)
                   ZSTM
                     .foreach(request.requestItems.toList) { case (table, requests) =>
                       ZSTM.foreach(requests.toList) { req =>
                         (req.putRequest.toOption, req.deleteRequest.toOption) match {
                           case (Some(put), _) =>
                             store.update(_.addItem(table, put.item)) *> ZSTM.succeed(
                               BatchWriteItemResponse(unprocessedItems = Map.empty[TableName, Iterable[WriteRequest]])
                             )
                           case (_, Some(del)) =>
                             store.update(_.removeItem(table, del.key)) *> ZSTM.succeed(
                               BatchWriteItemResponse(unprocessedItems = Map.empty[TableName, Iterable[WriteRequest]])
                             )
                           case _ =>
                             store.get *> ZSTM.succeed(
                               BatchWriteItemResponse(unprocessedItems = Map.empty[TableName, Iterable[WriteRequest]])
                             )
                         }
                       }
                     }
                     .map(_.flatten)
                 else resourceNotFound(s"One or more ${requestedTables.mkString(", ")} not found")
        } yield res
      }

    def createTable(request: CreateTableRequest): ZIO[Any, AwsError, Unit] =
      ZSTM.atomically {
        for {
          tableAlreadyExists <- checkTableExists(request.tableName)
          _ <- if (tableAlreadyExists)
                 ZSTM.fail(
                   AwsError.fromThrowable(
                     ResourceInUseException.builder.message(s"${request.tableName} already exists").build()
                   )
                 )
               else store.update(_.addTable(request.tableName))
        } yield ()
      }

    def deleteItem(request: DeleteItemRequest): ZIO[Any, AwsError, Unit] =
      ZSTM.atomically {
        for {
          tableExists <- checkTableExists(request.tableName)
          _ <- if (tableExists) store.update(_.removeItem(request.tableName, request.key))
               else resourceNotFound(s"${request.tableName} not found")
        } yield ()
      }

    def deleteTable(request: DeleteTableRequest): ZIO[Any, AwsError, Unit] =
      ZSTM.atomically {
        for {
          tableExists <- checkTableExists(request.tableName)
          _ <- if (tableExists) store.update(_.removeTable(request.tableName))
               else resourceNotFound(s"${request.tableName} not found")
        } yield ()
      }

    def describeTable(request: DescribeTableRequest): ZIO[Any, AwsError, TableDescription] =
      ZSTM.atomically {
        for {
          tableExists <- checkTableExists(request.tableName)
          response     = TableDescription(tableName = request.tableName)
          table       <- if (tableExists) ZSTM.succeed(response) else resourceNotFound(s"${request.tableName} not found")
        } yield table

      }

    def getItem(request: GetItemRequest): ZIO[Any, AwsError, GetItemResponse] =
      ZSTM.atomically {
        for {
          tableExists <- checkTableExists(request.tableName)
          item <- if (tableExists) store.get.map(_.fetchItem(request.tableName, request.key))
                  else resourceNotFound(s"${request.tableName} not found")
        } yield GetItemResponse(Some(item.getOrElse(Map.empty)))
      }

    def listTables(request: ListTablesRequest): ZIO[Any, AwsError, List[TableName]] =
      ZSTM.atomically {
        for {
          tableExists <- request.exclusiveStartTableName.toOption.fold(ZSTM.succeed(true))(checkTableExists)
          tables <- if (tableExists) store.get.map(_.keys.toList)
                    else resourceNotFound(s"${request.exclusiveStartTableName.toOption.getOrElse("")} not found")
          enforceLimit <-
            ZSTM.fromOption(request.limit.toOption).map(limit => tables.take(limit)).orElse(ZSTM.succeed(tables))
        } yield enforceLimit
      }

    def putItem(request: PutItemRequest): ZIO[Any, AwsError, Unit] =
      ZSTM.atomically {
        for {
          tableExists <- checkTableExists(request.tableName)
          _ <- if (tableExists) store.update(_.addItem(request.tableName, request.item))
               else resourceNotFound(s"${request.tableName} not found")
        } yield ()
      }

    def query(request: QueryRequest): ZIO[Any, AwsError, List[Map[AttributeName, AttributeValue]]] =
      ZSTM.atomically {
        for {
          tableExists <- checkTableExists(request.tableName)
          // sort of giving up here, having to implement the entire query api for dynamo feels a bit much
          filters = request.exclusiveStartKey.toList
          items <- if (tableExists) store.get.map(_.fetchItems(request.tableName, filters))
                   else resourceNotFound(s"${request.tableName} not found")
        } yield items.getOrElse(List.empty)
      }

    def scan(request: ScanRequest): ZIO[Any, AwsError, List[Map[AttributeName, AttributeValue]]] =
      ZSTM.atomically {
        for {
          tableExists <- checkTableExists(request.tableName)
          // same goes for the scan api, regarding the
          items <- if (tableExists) store.get.map(_.fetchItems(request.tableName, request.exclusiveStartKey.toList))
                   else resourceNotFound(s"${request.tableName} not found")
        } yield items.getOrElse(List.empty)
      }

    // Trivially succeeds if table exists
    def updateTable(request: UpdateTableRequest): ZIO[Any, AwsError, Unit] =
      ZSTM.atomically {
        for {
          tableExists <- checkTableExists(request.tableName)
          _           <- if (tableExists) ZSTM.succeed(()) else resourceNotFound(s"One or more ${request.tableName} not found")
        } yield ()
      }

    def updateItem(request: UpdateItemRequest): ZIO[Any, AwsError, Unit] =
      ZSTM.atomically {
        for {
          tableExists <- checkTableExists(request.tableName)
          itemExists  <- store.get.map(_.itemExists(request.tableName, request.key))
          _ <- if (tableExists && itemExists)
                 store.update(_.replaceItem(request.tableName, request.key, request.attributeUpdates.toOption))
               else resourceNotFound(s"${request.tableName} or ${request.key} not found")
        } yield ()
      }

    private def checkTableExists(tableName: TableName): ZSTM[Any, Nothing, Boolean] =
      store.get.map(_.keys.toList.contains(tableName))

    private def resourceNotFound(message: String): STM[AwsError, Nothing] =
      ZSTM.fail(
        AwsError.fromThrowable(
          ResourceNotFoundException.builder.message(message).build()
        )
      )

  }

  implicit class TestDBOps(private val underlying: DB) extends AnyVal {
    def addItem(key: TableName, item: Map[AttributeName, AttributeValue]): DB =
      underlying.updatedWith(key)(_.map(_.fold(Some(Chunk(item)))(items => Some(items :+ item))))
    def addTable(key: TableName): DB = underlying + (key -> None)

    def fetchItem(
      table: TableName,
      key: Map[AttributeName, AttributeValue]
    ): Option[Map[AttributeName, AttributeValue]] =
      for {
        items     <- underlying.get(table)
        maybeItem <- items.flatMap(_.filter(item => key.forall(item.toList.contains)).headOption)
      } yield maybeItem

    def fetchItems(
      table: TableName,
      filters: List[Map[AttributeName, AttributeValue]]
    ): Option[List[Map[AttributeName, AttributeValue]]] =
      for {
        items     <- underlying.get(table)
        maybeItem <- items.map(_.filter(item => filters.flatMap(_.toList).forall(item.toList.contains)).toList)
      } yield maybeItem

    def itemExists(table: TableName, key: Map[AttributeName, AttributeValue]): Boolean =
      underlying
        .get(table)
        .flatMap(_.map(_.filter(item => key.forall(item.toList.contains)).nonEmpty))
        .getOrElse(false)

    def removeItem(key: TableName, item: Map[AttributeName, AttributeValue]): DB = {
      val updated: Option[DB] = for {
        maybeTable <- underlying.get(key)
        maybeItems <- maybeTable
        filtered    = maybeItems.filterNot(_ == item)
      } yield underlying.updated(key, Option(filtered))

      updated.getOrElse(underlying)
    }

    def removeTable(key: TableName): DB = underlying - key

    // Does not support destructive updates
    def replaceItem(
      table: TableName,
      key: Map[AttributeName, AttributeValue],
      replaceWith: Option[Map[AttributeName, AttributeValueUpdate]]
    ): DB = {
      val removed = removeItem(table, key)
      replaceWith.map { update =>
        val item: Map[AttributeName, AttributeValue] =
          key ++ update.view
            .mapValues(_.value.toOption)
            .collect { case (key, Some(value)) => key -> value }
            .toMap

        removed.addItem(table, item)
      }.getOrElse(underlying)
    }
  }

  val layer: ULayer[TestDynamoDBConnector] = ZLayer.fromZIO {
    ZSTM.atomically {
      for {
        store <- TRef.make(Map.empty[TableName, Option[Chunk[Map[AttributeName, AttributeValue]]]])
        db     = TestDynamoDb(store)
      } yield TestDynamoDBConnector(db)
    }
  }
}
