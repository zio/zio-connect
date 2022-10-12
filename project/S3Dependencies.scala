import sbt._

object S3Dependencies {

  val awsCore        = "com.amazonaws"      % "aws-java-sdk-core" % "1.12.319"
  val testContainers = "org.testcontainers" % "localstack"        % "1.17.5" % "test"

  val zioAwsVersion = "5.17.280.1"
  val zioAwsNetty   = "dev.zio" %% "zio-aws-netty" % zioAwsVersion
  val zioAwsS3      = "dev.zio" %% "zio-aws-s3"    % zioAwsVersion

}
