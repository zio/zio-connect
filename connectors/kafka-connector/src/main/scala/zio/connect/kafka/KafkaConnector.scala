package zio.connect.kafka

import org.apache.kafka.clients.producer.ProducerRecord
import zio.Trace
import zio.connect.kafka.KafkaConnector._
import zio.kafka.admin.AdminClient.{NewTopic, TopicListing}
import zio.kafka.consumer._
import zio.kafka.serde.{Deserializer, Serde, Serializer}
import zio.stream._

trait KafkaConnector {

  def createTopic(implicit trace: Trace): ZSink[Any, Throwable, NewTopic, Nothing, Unit]

  def listTopics(implicit trace: Trace): ZStream[Any, Throwable, TopicListing]

  final def publishKeyValue[K, V](topic: => String)(implicit
    keySerializer: Serializer[Any, K],
    valueSerializer: Serializer[Any, V],
    trace: Trace
  ): ZSink[Any, Throwable, KeyValue[K, V], KeyValue[K, V], Unit] =
    publishRecord[K, V]
      .contramap[KeyValue[K, V]](kv => new ProducerRecord(topic, kv.key, kv.value))
      .channel
      .mapOut(_.map(kv => KeyValue(kv.key, kv.value)))
      .toSink

  final def publishPartitionTopicKeyValue[K, V](implicit
    keySerializer: Serializer[Any, K],
    valueSerializer: Serializer[Any, V],
    trace: Trace
  ): ZSink[Any, Throwable, PartitionTopicKeyValue[K, V], PartitionTopicKeyValue[K, V], Unit] =
    publishRecord[K, V]
      .contramap[PartitionTopicKeyValue[K, V]](ptkv =>
        new ProducerRecord(ptkv.topic, ptkv.partition, ptkv.key, ptkv.value)
      )
      .channel
      .mapOut(_.map(ptkv => PartitionTopicKeyValue(ptkv.topic, ptkv.partition, ptkv.key, ptkv.value)))
      .toSink

  def publishRecord[K, V](implicit
    keySerializer: Serializer[Any, K],
    valueSerializer: Serializer[Any, V],
    trace: Trace
  ): ZSink[Any, Throwable, ProducerRecord[K, V], ProducerRecord[K, V], Unit]

  final def publishTopicKeyValue[K, V](implicit
    keySerializer: Serializer[Any, K],
    valueSerializer: Serializer[Any, V],
    trace: Trace
  ): ZSink[Any, Throwable, TopicKeyValue[K, V], TopicKeyValue[K, V], Unit] =
    publishRecord[K, V]
      .contramap[TopicKeyValue[K, V]](tkv => new ProducerRecord(tkv.topic, tkv.key, tkv.value))
      .channel
      .mapOut(_.map(tkv => TopicKeyValue(tkv.topic, tkv.key, tkv.value)))
      .toSink

  final def publishValue[V](topic: => String)(implicit
    serializer: Serializer[Any, V],
    trace: Trace
  ): ZSink[Any, Throwable, V, V, Unit] = {
    implicit val keySerde: Serde[Any, String] = Serde.string
    publishRecord[String, V].contramap[V](v => new ProducerRecord(topic, v)).channel.mapOut(_.map(_.value)).toSink
  }

  def read[K, V](topic: => String)(implicit
    keyDeserializer: Deserializer[Any, K],
    valueDeserializer: Deserializer[Any, V],
    trace: Trace
  ): ZStream[Any, Throwable, CommittableRecord[K, V]]

}

object KafkaConnector {
  final case class KeyValue[K, V](key: K, value: V)
  final case class TopicKeyValue[K, V](topic: String, key: K, value: V)
  final case class PartitionTopicKeyValue[K, V](topic: String, partition: Int, key: K, value: V)
}
