import sbt._

object Dependencies {

  val `scala-compact-collection` = "org.scala-lang.modules" %% "scala-collection-compat" % "2.8.1"

  val silencerVersion = "1.7.9"

  val zioVersion     = "2.0.2"
  val zio            = "dev.zio" %% "zio"          % zioVersion
  val `zio-streams`  = "dev.zio" %% "zio-streams"  % zioVersion
  val `zio-test`     = "dev.zio" %% "zio-test"     % zioVersion % "test"
  val `zio-test-sbt` = "dev.zio" %% "zio-test-sbt" % zioVersion % "test"
}
