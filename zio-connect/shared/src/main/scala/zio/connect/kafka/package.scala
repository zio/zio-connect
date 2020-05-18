package zio.connect

import zio._
import zio.stream._

package object kafka {
// Powered by ZIO Kafka
  type KafkaSettings 
  type KafkaConnector = Has[KafkaConnector.Service]
  type ConnectionError 

  object KafkaConnector {
    trait Service 
  }

  import java.io.IOException

  def kafkaLayer: ZLayer[KafkaSettings, ConnectionError, KafkaConnector] = ???

  def subscribe(topics: String): ZStream[KafkaConnector, IOException, Byte] = ???
}

// ZIO S3, ZIO FTP
