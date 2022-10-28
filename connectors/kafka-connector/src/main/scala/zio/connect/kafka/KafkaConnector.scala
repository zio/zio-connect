package zio.connect.kafka

import org.apache.kafka.clients.producer.{ProducerRecord}
import org.apache.kafka.common.Uuid
import zio.Trace
import zio.connect.kafka.KafkaConnector._
import zio.kafka.consumer._
import zio.stream._

trait KafkaConnector {
  def createTopic(implicit trace: Trace): ZSink[Any, Throwable, NewTopic, Nothing, Unit]

  def listTopics(implicit trace: Trace): ZStream[Any, Throwable, TopicListing]

  def read(topic: => String)(implicit trace: Trace): ZStream[Any, Throwable, CommittableRecord[String, String]]
  def write(implicit trace: Trace): ZSink[Any, Throwable, ProducerRecord[String, String], ProducerRecord[String, String], Unit]
}

object KafkaConnector {

  final case class NewTopic(
    name: String,
    numPartitions: Int,
    replicationFactor: Short,
    configs: Map[String, String] = Map()
  )

  final case class TopicListing(name: String, topicId: Uuid, isInternal: Boolean)

}
