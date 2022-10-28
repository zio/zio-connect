import sbt._

object FtpDependencies {

  val zioFtpVersion = "0.4.0"
  val testContainersVersion = "1.17.5"
  // test containers
  lazy val testContainers        = "org.testcontainers" % "testcontainers" % testContainersVersion % "test"
  lazy val testContainersJupiter = "org.testcontainers" % "junit-jupiter"  % testContainersVersion % "test"
  lazy val `junit-jupiter`       = "org.junit.jupiter"  % "junit-jupiter"  % "5.8.1"               % "test"

  val zioFtp = "dev.zio" %% "zio-ftp" % zioFtpVersion

}
