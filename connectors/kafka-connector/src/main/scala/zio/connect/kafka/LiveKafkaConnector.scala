package zio.connect.kafka

import org.apache.kafka.clients.producer.ProducerRecord
import zio._
import zio.kafka.admin.AdminClient.{NewTopic, TopicListing}
import zio.kafka.admin._
import zio.kafka.consumer._
import zio.kafka.producer._
import zio.kafka.serde._
import zio.stream._

case class LiveKafkaConnector(adminClient: AdminClient, consumer: Consumer, producer: Producer) extends KafkaConnector {

  def createTopic(implicit trace: Trace): ZSink[Any, Throwable, NewTopic, Nothing, Unit] =
    ZSink
      .foreach[Any, Throwable, NewTopic] { newTopic =>
        adminClient.createTopic(
          NewTopic(
            name = newTopic.name,
            numPartitions = newTopic.numPartitions,
            replicationFactor = newTopic.replicationFactor,
            configs = newTopic.configs
          )
        )
      }

  def listTopics(implicit trace: Trace): ZStream[Any, Throwable, TopicListing] =
    ZStream.fromIterableZIO {
      adminClient.listTopics().map(_.values)
    }

  override def publishRecord[K, V](implicit
    keySerde: Serializer[Any, K],
    valueSerde: Serializer[Any, V],
    trace: Trace
  ): ZSink[Any, Throwable, ProducerRecord[K, V], ProducerRecord[K, V], Unit] =
    ZSink.foreachChunk[Any, Throwable, ProducerRecord[K, V]] { records =>
      producer.produceChunk(records, keySerde, valueSerde)
    }

  def read[K, V](topic: => String)(implicit
    keyDeserializer: Deserializer[Any, K],
    valueDeserializer: Deserializer[Any, V],
    trace: Trace
  ): ZStream[Any, Throwable, CommittableRecord[K, V]] =
    consumer
      .subscribeAnd(Subscription.Topics(Set(topic)))
      .plainStream(keyDeserializer, valueDeserializer)
}

object LiveKafkaConnector {
  val layer: ZLayer[AdminClient with Consumer with Producer, Nothing, KafkaConnector] =
    ZLayer {
      for {
        adminClient <- ZIO.service[AdminClient]
        consumer    <- ZIO.service[Consumer]
        producer    <- ZIO.service[Producer]
      } yield LiveKafkaConnector(adminClient, consumer, producer)
    }

}
