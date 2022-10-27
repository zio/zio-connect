import sbt._

object CassandraDependencies {

  object version {
    val cassandraVersion     = "4.15.0"
    val cassandraUnitVersion = "3.7.1.1-SNAPSHOT"
  }

  lazy val cassandraConnector    = "com.datastax.oss"   % "java-driver-core"          % version.cassandraVersion
  lazy val cassandraQueryBuilder = "com.datastax.oss"   % "java-driver-query-builder" % version.cassandraVersion
  lazy val cassandraContainer    = "org.testcontainers" % "cassandra"                 % "1.17.5" % Test

}
