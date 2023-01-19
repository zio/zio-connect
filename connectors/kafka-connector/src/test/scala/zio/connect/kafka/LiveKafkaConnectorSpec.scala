package zio.connect.kafka

import zio._
import zio.kafka.KafkaTestUtils
import zio.kafka.KafkaTestUtils._
import zio.kafka.admin.AdminClient
import zio.kafka.embedded.Kafka
import zio.test.TestAspect._

import java.util.UUID

object LiveKafkaConnectorSpec extends KafkaConnectorSpec {

  override def spec = connectorSpec
    .provideSome[Kafka with AdminClient](
      zio.connect.kafka.kafkaConnectorLiveLayer,
      consumer(UUID.randomUUID().toString, Some(UUID.randomUUID().toString)),
      producer
    )
    .provideShared(
      Kafka.embedded,
      ZLayer.fromZIO(KafkaTestUtils.adminSettings),
      AdminClient.live
    ) @@ withLiveClock @@ timeout(
    300.seconds
  )

}
