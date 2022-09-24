import sbt.Keys.scalaVersion
import sbt._

object Dependencies {

  val ScalaCompactCollectionVersion = "2.8.1"
  val `scala-compact-collection` = "org.scala-lang.modules" %% "scala-collection-compat" % ScalaCompactCollectionVersion

}
