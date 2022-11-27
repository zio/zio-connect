import sbt._

object SqsDependencies {

  lazy val `zio-prelude` = "dev.zio" %% "zio-prelude" % "1.0.0-RC16"

  lazy val `aws-java-sdk-core` = "com.amazonaws" % "aws-java-sdk" % "1.12.319"

  lazy val localstack = "org.testcontainers" % "localstack" % "1.17.5" % "test"
}
