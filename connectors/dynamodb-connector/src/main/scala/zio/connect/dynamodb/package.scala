package zio.connect

import zio.aws.core.AwsError
import zio.aws.dynamodb.DynamoDb
import zio.aws.dynamodb.model.primitives.TableName
import zio.aws.dynamodb.model._
import zio.stream.{ZSink, ZStream}
import zio.{RLayer, Trace, ULayer}

package object dynamodb {

  def createTable(implicit
    trace: Trace
  ): ZSink[DynamoDBConnector, AwsError, CreateTableRequest, CreateTableRequest, Unit] =
    ZSink.serviceWithSink[DynamoDBConnector](_.createTable)

  def describeTable(request: => TableName)(implicit
    trace: Trace
  ): ZStream[DynamoDBConnector, AwsError, TableDescription] =
    ZStream.serviceWithStream[DynamoDBConnector](_.describeTable(request))

  def getItem(request: => GetItemRequest)(implicit
    trace: Trace
  ): ZStream[DynamoDBConnector, AwsError, GetItemResponse] =
    ZStream.serviceWithStream[DynamoDBConnector](_.getItem(request))

  def putItem(implicit trace: Trace): ZSink[DynamoDBConnector, AwsError, PutItemRequest, PutItemRequest, Unit] =
    ZSink.serviceWithSink[DynamoDBConnector](_.putItem)

  def tableExists(implicit trace: Trace): ZSink[DynamoDBConnector, AwsError, TableName, TableName, Boolean] =
    ZSink.serviceWithSink[DynamoDBConnector](_.tableExists)

  val dynamoDBConnectorLiveLayer: RLayer[DynamoDb, LiveDynamoDBConnector] = LiveDynamoDBConnector.layer
  val dynamoDBConnectorTestLayer: ULayer[TestDynamoDBConnector]           = TestDynamoDBConnector.layer
}
