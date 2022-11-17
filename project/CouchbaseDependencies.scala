import sbt._

object CouchbaseDependencies {

  lazy val couchbase = "com.couchbase.client" %% "scala-client" % "1.4.0"
  lazy val couchbaseContainer = "org.testcontainers" % "couchbase" % "1.17.5" % "test"

  lazy val `zio-prelude` = "dev.zio" %% "zio-prelude" % "1.0.0-RC16"

}
