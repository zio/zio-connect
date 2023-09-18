import sbt._

object KinesisDataStreamsDependencies {

  //required for localstack testcontainer
  lazy val `aws-java-sdk-core` = "com.amazonaws" % "aws-java-sdk-core" % "1.12.552" % "test"

  lazy val localstack = "org.testcontainers" % "localstack" % "1.17.5" % "test"

  lazy val `zio-aws-kinesis` = "nl.vroste" %% "zio-kinesis" % "0.30.1"
}
