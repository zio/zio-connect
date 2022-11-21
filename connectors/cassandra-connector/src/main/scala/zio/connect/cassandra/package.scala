package zio.connect

import zio.Trace
import zio.connect.cassandra.CassandraConnector.{CassandraException, CreateKeySpaceObject}
import zio.stream.ZSink

package object cassandra {
  def createKeyspace(implicit
    trace: Trace
  ): ZSink[CassandraConnector, CassandraException, CreateKeySpaceObject, Nothing, Unit] =
    ZSink.serviceWithSink(_.createKeyspace)

  def deleteKeyspace(implicit trace: Trace): ZSink[CassandraConnector, CassandraException, String, Nothing, Unit] =
    ZSink.serviceWithSink(_.deleteKeyspace)

  val cassandraConnectorLiveLayer = LiveCassandraConnector.layer
  val cassandraConnectorTestLayer = TestCassandraConnector.layer
}
