package zio.connect.couchbase

import com.couchbase.client.core.env.TimeoutConfig
import com.couchbase.client.java.env.ClusterEnvironment
import com.couchbase.client.java.{Cluster, ClusterOptions}
import org.testcontainers.couchbase.{BucketDefinition, CouchbaseContainer}
import org.testcontainers.utility.DockerImageName
import zio.{ZIO, ZLayer, durationInt}

object LiveCouchbaseConnectorSpec extends CouchbaseConnectorSpec {

  override def spec =
    suite("LiveCouchbaseConnectorSpec")(couchbaseConnectorSpec)
      .provideShared(
        couchbaseContainer,
        cluster,
        zio.connect.couchbase.couchbaseConnectorLiveLayer
      )

  private lazy val couchbaseContainer: ZLayer[Any, Throwable, CouchbaseContainer] =
    ZLayer.scoped(
      ZIO.acquireRelease(ZIO.attempt {
        val dockerImageName = DockerImageName
          .parse("couchbase:enterprise")
          .asCompatibleSubstituteFor("couchbase/server")
        val container = new CouchbaseContainer(dockerImageName)
          .withBucket(new BucketDefinition("CouchbaseConnectorBucket"))
        container.start()
        container
      }.retryN(4))(container => ZIO.attempt(container.stop()).orDie)
    )

  private lazy val cluster: ZLayer[CouchbaseContainer, Throwable, Cluster] = {
    val timeout = 5.seconds
    ZLayer
      .fromZIO(for {
        container <- ZIO.service[CouchbaseContainer]
        env =
          ClusterEnvironment
            .builder()
            .timeoutConfig(TimeoutConfig.kvTimeout(timeout).queryTimeout(timeout).connectTimeout(timeout))
            .build()
        cluster <- ZIO.attempt(
                     Cluster.connect(
                       container.getConnectionString,
                       ClusterOptions.clusterOptions(container.getUsername, container.getPassword).environment(env)
                     )
                   )
      } yield cluster)
  }

}
