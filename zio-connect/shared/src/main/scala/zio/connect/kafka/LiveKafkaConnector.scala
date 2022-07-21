package zio.connect.kafka

import zio.ZLayer
import zio.connect.kafka.LiveKafkaConnector.KafkaSettings
import zio.stream.ZStream

import java.io.IOException

// Powered by ZIO Kafka
case class LiveKafkaConnector(kafkaSettings: KafkaSettings) extends KafkaConnector {
  override def subscribe(topic: String): ZStream[Any, IOException, Byte] = ???
}

object LiveKafkaConnector {

  type KafkaSettings
  type ConnectionError

  val layer: ZLayer[KafkaSettings, ConnectionError, KafkaConnector] = ???

}