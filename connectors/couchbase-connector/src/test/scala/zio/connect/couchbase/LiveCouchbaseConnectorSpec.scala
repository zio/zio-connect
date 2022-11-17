package zio.connect.couchbase

import com.couchbase.client.java.Cluster
import org.testcontainers.couchbase.{BucketDefinition, CouchbaseContainer}
import org.testcontainers.utility.DockerImageName
import zio.{ZIO, ZLayer}

object LiveCouchbaseConnectorSpec extends CouchbaseConnectorSpec {

  override def spec =
    suite("LiveCouchbaseConnectorSpec")(couchbaseConnectorSpec)
      .provideShared(
        couchbaseContainer,
        cluster,
        zio.connect.couchbase.couchbaseConnectorLiveLayer
      )

  lazy val couchbaseContainer: ZLayer[Any, Throwable, CouchbaseContainer] =
    ZLayer.scoped(
      ZIO.acquireRelease(ZIO.attempt {
        val dockerImageName = DockerImageName
          .parse("couchbase:enterprise")
          .asCompatibleSubstituteFor("couchbase/server")
        val container = new CouchbaseContainer(dockerImageName)
          .withBucket(new BucketDefinition("CouchbaseConnectorBucket"))
        container.start()
        container
      })(container => ZIO.attempt(container.stop()).orDie)
    )

  lazy val cluster: ZLayer[CouchbaseContainer, Throwable, Cluster] =
    ZLayer
      .fromZIO(for {
        container <- ZIO.service[CouchbaseContainer]
        cluster <- ZIO.attempt(
                     Cluster.connect(
                       container.getConnectionString,
                       container.getUsername,
                       container.getPassword
                     )
                   )
      } yield cluster)

}
