import sbt._

object DynamoDBDependencies {
  //required for localstack testcontainer
  lazy val `aws-java-sdk-core` = "com.amazonaws" % "aws-java-sdk-core" % "1.12.319" % "test"

  lazy val localstack = "org.testcontainers" % "localstack" % "1.17.6" % "test"

  lazy val zioAwsVersion      = "5.17.295.10"
  lazy val `zio-aws-netty`    = "dev.zio" %% "zio-aws-netty"    % zioAwsVersion % "test"
  lazy val `zio-aws-dynamodb` = "dev.zio" %% "zio-aws-dynamodb" % zioAwsVersion
}
