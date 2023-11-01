import sbt._

object S3Dependencies {

  // required for localstack testcontainer
  lazy val `aws-java-sdk-core` = "com.amazonaws" % "aws-java-sdk-core" % "1.12.319" % "test"

  lazy val localstack = "org.testcontainers" % "localstack" % "1.17.5" % "test"

  lazy val zioAwsVersion   = "5.17.280.1"
  lazy val `zio-aws-netty` = "dev.zio" %% "zio-aws-netty" % zioAwsVersion
  lazy val `zio-aws-s3`    = "dev.zio" %% "zio-aws-s3"    % zioAwsVersion

}
