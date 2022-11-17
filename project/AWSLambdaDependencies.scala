import sbt._

object AWSLambdaDependencies {

  //required for localstack testcontainer
  lazy val `aws-java-sdk-core` = "com.amazonaws" % "aws-java-sdk-core" % "1.12.319" % "test"

  lazy val localstack = "org.testcontainers" % "localstack" % "1.17.5" % "test"

  lazy val zioAwsVersion    = "5.17.295.6"
  lazy val `zio-aws-netty`  = "dev.zio" %% "zio-aws-netty"  % zioAwsVersion
  lazy val `zio-aws-lambda` = "dev.zio" %% "zio-aws-lambda" % zioAwsVersion

  lazy val `zio-http` = "dev.zio" %% "zio-http" % "0.0.2+1-cac1e988-SNAPSHOT"
//  lazy val `zio-http`         = "dev.zio" %% "zio-http"         % "2.0.0-RC11+152-ac2bf3cc-SNAPSHOT"
  lazy val `zio-http-logging` = "dev.zio" %% "zio-http-logging" % "0.0.2+1-cac1e988-SNAPSHOT"
}
