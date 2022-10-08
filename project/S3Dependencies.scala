import sbt._

object S3Dependencies {

  val zioAwsVersion = "5.17.280.1"

  val zioAwsS3   = "dev.zio" %% "zio-aws-s3"   % zioAwsVersion
}
