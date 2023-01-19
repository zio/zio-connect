package zio.connect

import org.apache.kafka.clients.producer.ProducerRecord
import zio.Trace
import zio.connect.kafka.KafkaConnector.{KeyValue, PartitionTopicKeyValue, TopicKeyValue}
import zio.kafka.admin.AdminClient.{NewTopic, TopicListing}
import zio.kafka.consumer._
import zio.kafka.serde.{Deserializer, Serializer}
import zio.stream._

package object kafka {

  def createTopic: ZSink[KafkaConnector, Throwable, NewTopic, Nothing, Unit] =
    ZSink.serviceWithSink(_.createTopic)

  def listTopics: ZStream[KafkaConnector, Throwable, TopicListing] =
    ZStream.serviceWithStream(_.listTopics)

  def read[K, V](topic: => String)(implicit
    keyDeserializer: Deserializer[Any, K],
    valueDeserializer: Deserializer[Any, V],
    trace: Trace
  ): ZStream[KafkaConnector, Throwable, CommittableRecord[K, V]] =
    ZStream.serviceWithStream(_.read(topic))

  def publishKeyValue[K, V](topic: => String)(implicit
    keySerializer: Serializer[Any, K],
    valueSerializer: Serializer[Any, V],
    trace: Trace
  ): ZSink[KafkaConnector, Throwable, KeyValue[K, V], KeyValue[K, V], Unit] =
    ZSink.serviceWithSink(_.publishKeyValue(topic))

  def publishPartitionTopicKeyValue[K, V](implicit
    keySerializer: Serializer[Any, K],
    valueSerializer: Serializer[Any, V],
    trace: Trace
  ): ZSink[KafkaConnector, Throwable, PartitionTopicKeyValue[K, V], PartitionTopicKeyValue[K, V], Unit] =
    ZSink.serviceWithSink(_.publishPartitionTopicKeyValue)

  def publishRecord[K, V](implicit
    keySerializer: Serializer[Any, K],
    valueSerializer: Serializer[Any, V],
    trace: Trace
  ): ZSink[KafkaConnector, Throwable, ProducerRecord[K, V], ProducerRecord[K, V], Unit] =
    ZSink.serviceWithSink(_.publishRecord)

  def publishTopicKeyValue[K, V](implicit
    keySerializer: Serializer[Any, K],
    valueSerializer: Serializer[Any, V],
    trace: Trace
  ): ZSink[KafkaConnector, Throwable, TopicKeyValue[K, V], TopicKeyValue[K, V], Unit] =
    ZSink.serviceWithSink(_.publishTopicKeyValue[K, V])

  def publishValue[V](topic: => String)(implicit
    serializer: Serializer[Any, V],
    trace: Trace
  ): ZSink[KafkaConnector, Throwable, V, V, Unit] =
    ZSink.serviceWithSink(_.publishValue(topic))

  val kafkaConnectorLiveLayer = LiveKafkaConnector.layer

}
