package zio.connect

import zio.Trace
import zio.connect.kafka.KafkaConnector.KeyValue
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

  val kafkaConnectorLiveLayer = LiveKafkaConnector.layer

}
