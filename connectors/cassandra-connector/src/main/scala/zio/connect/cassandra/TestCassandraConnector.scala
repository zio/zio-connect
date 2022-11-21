package zio.connect.cassandra

import zio.connect.cassandra.CassandraConnector.{CassandraException, CreateKeySpaceObject}
import zio.connect.cassandra.TestCassandraConnector.CassandraObject.{CassandraKeyspace, CassandraTable}
import zio.connect.cassandra.TestCassandraConnector.TestCqlSession
import zio.stm.{STM, TRef, ZSTM}
import zio.stream.ZSink
import zio.{Trace, ZIO, ZLayer}

case class TestCassandraConnector(session: TestCqlSession) extends CassandraConnector {

  override def createKeyspace(implicit
    trace: Trace
  ): ZSink[Any, CassandraException, CreateKeySpaceObject, Nothing, Unit] = ZSink
    .foreach[Any, CassandraException, CreateKeySpaceObject] { keyspace =>
      ZIO
        .attempt(
          session
            .createKeyspace(
              keyspace
            )
        )
        .mapError(e => CassandraException(e.getMessage, e))
    }

  override def deleteKeyspace(implicit trace: Trace): ZSink[Any, CassandraException, String, Nothing, Unit] =
    ZSink
      .foreach[Any, CassandraException, String] { keyspace =>
        ZIO
          .attempt(
            session
              .deleteKeyspace(keyspace)
          )
          .mapError(e => CassandraException(e.getMessage, e))
      }
}

object TestCassandraConnector {

  val layer: ZLayer[Any, Nothing, TestCassandraConnector] =
    ZLayer.fromZIO(STM.atomically {
      for {
        a <- TRef.make(Map.empty[String, CassandraKeyspace])
      } yield TestCassandraConnector(TestCqlSession(a))
    })

  private[cassandra] sealed trait CassandraObject

  private[cassandra] object CassandraObject {

    final case class CassandraKeyspace(name: String, tables: Seq[CassandraTable]) extends CassandraObject
    final case class CassandraTable(name: String, column: Map[String, String], objects: Map[String, String])
        extends CassandraObject
  }

  private[cassandra] final case class TestCqlSession(repo: TRef[Map[String, CassandraKeyspace]]) {

    def createKeyspace(keyspace: CreateKeySpaceObject): ZIO[Any, RuntimeException, Unit] = ZSTM.atomically(
      for {
        map <- repo.get
        _ <- ZSTM
               .fail(())
               .when(map.contains(keyspace.keyspace))
               .mapError(_ => new RuntimeException)
        _ <-
          repo.getAndUpdate(mp =>
            mp.updated(keyspace.keyspace, CassandraKeyspace(keyspace.keyspace, Seq[CassandraTable]()))
          )
      } yield ()
    )

    def deleteKeyspace(keyspace: String): ZIO[Any, RuntimeException, Unit] = ZSTM.atomically(
      for {
        map <- repo.get
        _ <- ZSTM
               .fail(())
               .when(!map.contains(keyspace))
               .mapError(_ => new RuntimeException)
        _ <- repo.getAndUpdate(m => m - keyspace)
      } yield ()
    )
  }

}
