package zio.connect.dynamodb

object TestDynamoDBConnectorSpec extends DynamoDBConnectorSpec {
  override def spec =
    suite("TestDynamoDBConnectorSpec")(dynamoDBConnectorSpec.provideShared(dynamoDBConnectorTestLayer))
}
