package zio.connect.couchbase

import com.couchbase.client.scala.env.{ClusterEnvironment, TimeoutConfig}
import com.couchbase.client.scala.{Cluster, ClusterOptions}
import org.testcontainers.couchbase.{BucketDefinition, CouchbaseContainer}
import org.testcontainers.utility.DockerImageName
import zio.{ZIO, ZLayer, durationInt}

import scala.jdk.DurationConverters.JavaDurationOps

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
    val timeout = 5.seconds.toScala
    ZLayer
      .fromZIO(for {
        container <- ZIO.service[CouchbaseContainer]
        env <- ZIO.fromTry(
                 ClusterEnvironment.builder
                   .timeoutConfig(TimeoutConfig().kvTimeout(timeout).queryTimeout(timeout).connectTimeout(timeout))
                   .build
               )
        cluster <- ZIO.fromTry(
                     Cluster.connect(
                       container.getConnectionString,
                       ClusterOptions.create(container.getUsername, container.getPassword).environment(env)
                     )
                   )
      } yield cluster)
  }

}
