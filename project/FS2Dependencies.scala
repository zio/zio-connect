import sbt._

object FS2Dependencies {

  lazy val `fs2-core` = "co.fs2" %% "fs2-core" % "3.3.0"

  lazy val `kind-projector` = "org.typelevel" % "kind-projector" % "0.13.2" cross CrossVersion.full

  lazy val `zio-interop-cats` = "dev.zio" %% "zio-interop-cats" % "3.3.0"

}
