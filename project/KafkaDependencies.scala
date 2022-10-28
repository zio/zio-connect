import sbt._

object KafkaDependencies {

  lazy val zioKafkaVersion   = "2.0.1"
  lazy val `zio-kafka` = "dev.zio" %% "zio-kafka" % zioKafkaVersion
  lazy val `zio-kafka-test-utils`    = "dev.zio" %% "zio-kafka-test-utils"    % zioKafkaVersion % "test"

}
