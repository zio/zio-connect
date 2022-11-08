package zio.connect.couchbase

object TestCouchbaseConnectorSpec extends CouchbaseConnectorSpec {

  override def spec =
    suite("TestCouchbaseConnectorSpec")(couchbaseConnectorSpec).provideLayer(couchbaseConnectorTestLayer)

}
