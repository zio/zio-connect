package zio.connect.kafka

import org.apache.kafka.clients.producer.ProducerRecord
import zio._
import zio.connect.kafka.KafkaConnector.{NewTopic, TopicListing}
import zio.kafka.admin.AdminClient.{NewTopic => AdminNewTopic}
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
          AdminNewTopic(
            name = newTopic.name,
            numPartitions = newTopic.numPartitions,
            replicationFactor = newTopic.replicationFactor,
            configs = newTopic.configs
          )
        )
      }

  def listTopics(implicit trace: Trace): ZStream[Any, Throwable, TopicListing] =
    ZStream
      .fromIterableZIO {
        adminClient.listTopics().map(_.map { case (_, topicListing) =>
          TopicListing(topicListing.name, topicListing.topicId, topicListing.isInternal)
        })
      }

  def read(topic: => String)(implicit trace: Trace): ZStream[Any, Throwable, CommittableRecord[String, String]] =
    consumer
      .subscribeAnd(Subscription.Topics(Set(topic)))
      .plainStream(Serde.string, Serde.string)
  override def write(implicit trace: Trace): ZSink[Any, Throwable, ProducerRecord[String, String], ProducerRecord[String, String], Unit] =
    ZSink.foreachChunk[Any, Throwable, ProducerRecord[String, String]] { records =>
      producer.produceChunk(records, Serde.string, Serde.string)
    }
}


object LiveKafkaConnector {
  val layer: ZLayer[AdminClient with Consumer with Producer, Nothing, KafkaConnector] =
    ZLayer {
      for {
        adminClient <- ZIO.service[AdminClient]
        consumer <- ZIO.service[Consumer]
        producer <- ZIO.service[Producer]
      } yield LiveKafkaConnector(adminClient, consumer, producer)
    }


}