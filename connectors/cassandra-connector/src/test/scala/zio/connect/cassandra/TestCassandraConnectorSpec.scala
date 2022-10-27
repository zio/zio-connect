package zio.connect.cassandra

object TestCassandraConnectorSpec extends CassandraConnectorSpec {

  override def spec = suite("TestCassandraConnectorSpec")(cassandraConnectorSpec).provide(cassandraConnectorTestLayer)
}
