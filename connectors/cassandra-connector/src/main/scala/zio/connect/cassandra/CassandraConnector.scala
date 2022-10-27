package zio.connect.cassandra

import zio.Trace
import zio.connect.cassandra.CassandraConnector.{CassandraException, CreateKeySpaceObject}
import zio.stream.ZSink

trait CassandraConnector {

  def createKeyspace(implicit trace: Trace): ZSink[Any, CassandraException, CreateKeySpaceObject, Boolean, Unit]

  def deleteKeyspace(implicit trace: Trace): ZSink[Any, CassandraException, String, Boolean, Unit]

}

object CassandraConnector {

  case class CassandraException(message: String, throwable: Throwable) extends RuntimeException(message, throwable)

  final case class CreateKeySpaceObject(
    keyspace: String,
    replicationClass: Option[String],
    replicationFactor: Option[Int]
  )

}
