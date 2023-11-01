package zio.connect.cassandra

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder
import zio.{Trace, ZIO, ZLayer}
import zio.connect.cassandra.CassandraConnector.{CassandraException, CreateKeySpaceObject}
import zio.stream.ZSink

case class LiveCassandraConnector(session: CqlSession) extends CassandraConnector {

  override def createKeyspace(implicit
    trace: Trace
  ): ZSink[Any, CassandraException, CreateKeySpaceObject, Nothing, Unit] = ZSink
    .foreach[Any, CassandraException, CreateKeySpaceObject] { keyspace =>
      ZIO
        .attempt(
          session
            .execute(
              SchemaBuilder
                .createKeyspace(keyspace.keyspace)
                .withSimpleStrategy(keyspace.replicationFactor.getOrElse(3))
                .build()
            )
            .wasApplied()
        )
        .mapError(e => CassandraException(e.getMessage, e))
        .flatMap(result =>
          ZIO
            .fail(CassandraException(s"Create keyspace $keyspace statement was not applied", new RuntimeException))
            .when(!result)
        )
    }

  override def deleteKeyspace(implicit trace: Trace): ZSink[Any, CassandraException, String, Nothing, Unit] =
    ZSink
      .foreach[Any, CassandraException, String] { keyspace =>
        ZIO
          .attempt(
            session
              .execute(
                SchemaBuilder
                  .dropKeyspace(keyspace)
                  .build()
              )
              .wasApplied()
          )
          .mapError(e => CassandraException(e.getMessage, e))
          .flatMap(result =>
            ZIO
              .fail(CassandraException(s"Create keyspace $keyspace statement was not applied", new RuntimeException))
              .when(!result)
              .orElse(ZIO.succeed(result))
          )
      }
}

object LiveCassandraConnector {
  val layer =
    ZLayer.fromZIO(ZIO.service[CqlSession].map(LiveCassandraConnector(_)))

}
