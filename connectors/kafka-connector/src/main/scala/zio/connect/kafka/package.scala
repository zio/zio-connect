package zio.connect

import org.apache.kafka.clients.producer.ProducerRecord
import zio.Trace
import zio.connect.kafka.KafkaConnector.{NewTopic, TopicListing}
import zio.kafka.consumer._
import zio.stream._

package object kafka {
  def createTopic: ZSink[KafkaConnector, Throwable, NewTopic, Nothing, Unit] =
    ZSink.serviceWithSink(_.createTopic)

  def listTopics: ZStream[KafkaConnector, Throwable, TopicListing] =
    ZStream.serviceWithStream(_.listTopics)

  def read(topic: => String): ZStream[KafkaConnector, Throwable, CommittableRecord[String, String]] =
    ZStream.serviceWithStream(_.read(topic))

  def write(implicit trace: Trace): ZSink[KafkaConnector, Throwable, ProducerRecord[String, String], ProducerRecord[String, String], Unit] =
    ZSink.serviceWithSink(_.write)


  val kafkaConnectorLiveLayer = LiveKafkaConnector.layer

}
