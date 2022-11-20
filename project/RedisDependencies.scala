import sbt._

object RedisDependencies {
  lazy val `zio-redis`           = "dev.zio" %% "zio-redis"           % "0.0.0+449-b7621f14-SNAPSHOT"
  lazy val `zio-config-magnolia` = "dev.zio" %% "zio-config-magnolia" % "3.0.2"
  lazy val `zio-config-typesafe` = "dev.zio" %% "zio-config-typesafe" % "3.0.2"
  lazy val `zio-schema-protobuf` = "dev.zio" %% "zio-schema-protobuf" % "0.2.1"
}
