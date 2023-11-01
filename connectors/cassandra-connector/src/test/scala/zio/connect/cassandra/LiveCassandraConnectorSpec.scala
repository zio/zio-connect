package zio.connect.cassandra

import com.datastax.oss.driver.api.core.CqlSession
import org.testcontainers.containers.CassandraContainer
import zio.{Scope, ZIO, ZLayer}

object LiveCassandraConnectorSpec extends CassandraConnectorSpec {

  override def spec =
    suite("LiveCassandraConnectorSpec")(cassandraConnectorSpec)
      .provideSomeShared[Scope](cassandraContainer, session, zio.connect.cassandra.cassandraConnectorLiveLayer)

  lazy val cassandraContainer: ZLayer[Scope, Throwable, CassandraContainer[_]] =
    ZLayer.fromZIO(
      ZIO.acquireRelease(ZIO.attempt {
        val cassandra = new CassandraContainer("cassandra:4.0")
        cassandra.start()
        cassandra
      })(c => ZIO.attempt(c.stop()).orDie)
    )

  lazy val session: ZLayer[CassandraContainer[_], Throwable, CqlSession] =
    ZLayer
      .fromZIO(for {
        cassandra <- ZIO.service[CassandraContainer[_]]
        session <- ZIO.succeed(
                     CqlSession
                       .builder()
                       .addContactPoint(cassandra.getContactPoint)
                       .withLocalDatacenter(cassandra.getLocalDatacenter)
                       .build()
                   )
      } yield session)
}
