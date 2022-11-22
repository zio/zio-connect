package zio.connect.kafka

import zio._
import zio.connect.kafka.KafkaConnector.KeyValue
import zio.kafka.KafkaTestUtils
import zio.kafka.KafkaTestUtils._
import zio.kafka.admin.AdminClient
import zio.kafka.admin.AdminClient.NewTopic
import zio.kafka.embedded.Kafka
import zio.kafka.serde.Serde
import zio.stream._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._

object LiveKafkaConnectorSpec extends ZIOSpecDefault {

  override def spec = (readSuite + writeSuite + topicSuite)
    .provideShared(Kafka.embedded, ZLayer.fromZIO(KafkaTestUtils.adminSettings), AdminClient.live)

  private lazy val readSuite =
    suite("read")(
      test("succeeds") {
        val tempTopic                                = "test-read-succeeds-topic"
        implicit val stringSerde: Serde[Any, String] = Serde.string
        val kvs                                      = (1 to 5).toList.map(i => (s"key$i", s"msg$i"))
        for {
          _ <- produceMany(tempTopic, kvs)
          records <- read(tempTopic)
                       .take(5)
                       .runCollect
          kvOut = records.map(r => (r.record.key, r.record.value)).toList
        } yield assert(kvOut)(equalTo(kvs))
      }.provideSome[Kafka with AdminClient](
        zio.connect.kafka.kafkaConnectorLiveLayer,
        consumer("test-read-succeeds-client", Some("test-read-succeeds-group")),
        producer
      )
    ) @@ withLiveClock @@ timeout(
      300.seconds
    )

  private lazy val writeSuite =
    suite("write")(
      test("succeeds") {
        val tempTopic                                = "test-write-succeeds-topic"
        implicit val stringSerde: Serde[Any, String] = Serde.string
        val kvs                                      = (1 to 5).toList.map(i => (s"key$i", s"msg$i"))
        for {
          _ <- ZStream.fromIterable(kvs.map { case (k, v) => new KeyValue[String, String](k, v) }) >>> publishKeyValue(
                 tempTopic
               )
          records <- read(tempTopic)
                       .take(5)
                       .runCollect
          kvOut = records.map(r => (r.record.key, r.record.value)).toList
        } yield assert(kvOut)(equalTo(kvs))
      }.provideSome[Kafka with AdminClient](
        zio.connect.kafka.kafkaConnectorLiveLayer,
        consumer("test-write-succeeds-client", Some("test-write-succeeds-group")),
        producer
      )
    ) @@ withLiveClock @@ timeout(
      300.seconds
    )

  private lazy val topicSuite =
    suite("topic")(
      test("create single topic") {
        val newTopic1 = NewTopic("test-topic-create-single-topic", 1, 1)
        for {
          list1 <- listTopics.filter(_.name.startsWith("test-topic-create")).runCollect
          _     <- ZStream.fromIterable(List(newTopic1)) >>> createTopic
          list2 <- listTopics.filter(_.name.startsWith("test-topic-create")).runCollect
        } yield assert(list1.size)(equalTo(0)) &&
          assert(list2.size)(equalTo(1))
      }
    ).provideSome[Kafka with AdminClient](
      zio.connect.kafka.kafkaConnectorLiveLayer,
      consumer("test-topic-create-single-topic-client", Some("test-topic-create-single-topic-group")),
      producer
    ) @@ withLiveClock @@ timeout(
      300.seconds
    )

}
