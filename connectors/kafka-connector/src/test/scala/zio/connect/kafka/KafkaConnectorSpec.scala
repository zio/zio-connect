package zio.connect.kafka

import zio.connect.kafka.KafkaConnector.{KeyValue, PartitionTopicKeyValue, TopicKeyValue}
import zio.kafka.admin.AdminClient.NewTopic
import zio.kafka.serde.Serde
import zio.stream._
import zio.test.Assertion._
import zio.test._

import java.util.UUID

trait KafkaConnectorSpec extends ZIOSpecDefault {

  val connectorSpec = readSuite + publishSuite + topicSuite

  private lazy val readSuite =
    suite("read")(test("succeeds") {
      val tempTopic                                = UUID.randomUUID().toString
      implicit val stringSerde: Serde[Any, String] = Serde.string
      val kvs                                      = (1 to 5).toList.map(i => KeyValue(s"key$i", s"msg$i"))
      val vs                                       = (6 to 10).toList.map(i => s"msg$i")
      for {
        _ <- ZStream(NewTopic(tempTopic, 1, 1)) >>> createTopic
        _ <- ZStream.fromIterable(kvs) >>> publishKeyValue(tempTopic)
        records <- read(tempTopic)
                     .take(5)
                     .runCollect
        kvOut1 = records.map(r => KeyValue(r.record.key, r.record.value)).toList

        _ <- ZStream.fromIterable(vs) >>> publishValue(tempTopic)
        records2 <- read(tempTopic)
                      .take(10)
                      .runCollect
                      .map(_.drop(5))
        kvOut2 = records2.map(r => r.record.value).toList
      } yield assert(kvOut1)(equalTo(kvs)) && assert(kvOut2)(equalTo(vs))
    })

  private lazy val topicSuite =
    suite("topic")(
      test("create single topic") {
        val tempTopicName = UUID.randomUUID().toString
        val newTopic1     = NewTopic(tempTopicName, 1, 1)
        for {
          list1 <- listTopics.filter(_.name == tempTopicName).runCollect
          _     <- ZStream.fromIterable(List(newTopic1)) >>> createTopic
          list2 <- listTopics.filter(_.name == tempTopicName).runCollect
        } yield assert(list1.size)(equalTo(0)) &&
          assert(list2.size)(equalTo(1))
      }
    )

  private lazy val publishSuite =
    suite("publish")(
      test("succeeds") {
        val tempTopic                                = UUID.randomUUID().toString
        implicit val stringSerde: Serde[Any, String] = Serde.string
        val kvs1                                     = (1 to 5).toList.map(i => KeyValue(s"key$i", s"msg$i"))
        val vs2                                      = (6 to 10).toList.map(i => s"msg$i")
        val kvs3                                     = (11 to 15).toList.map(i => TopicKeyValue(tempTopic, s"key$i", s"msg$i"))
        for {
          _ <- ZStream(NewTopic(tempTopic, numPartitions = 1, 1)) >>> createTopic
          _ <- ZStream.fromIterable(kvs1) >>> publishKeyValue(tempTopic)
          _ <- ZStream.fromIterable(vs2) >>> publishValue(tempTopic)
          _ <- ZStream.fromIterable(kvs3) >>> publishTopicKeyValue
          records <- read(tempTopic)
                       .take(15)
                       .runCollect
          kvOut1 = records.take(5).map(r => KeyValue(r.record.key, r.record.value)).toList
          kvOut2 = records.drop(5).take(5).map(r => r.record.value).toList
          kvOut3 = records.drop(10).take(5).map(r => TopicKeyValue(tempTopic, r.record.key, r.record.value)).toList
        } yield assert(kvOut1)(equalTo(kvs1)) &&
          assert(kvOut2)(equalTo(vs2)) &&
          assert(kvOut3)(equalTo(kvs3))
      },
      test("on multiple partitions succeeds") {
        val tempTopic                                = UUID.randomUUID().toString
        implicit val stringSerde: Serde[Any, String] = Serde.string
        val kvs_partition1                           = (16 to 20).toList.map(i => PartitionTopicKeyValue(tempTopic, 0, s"key$i", s"msg$i"))
        val kvs_partition2                           = (21 to 25).toList.map(i => PartitionTopicKeyValue(tempTopic, 1, s"key$i", s"msg$i"))
        for {
          _ <- ZStream(NewTopic(tempTopic, numPartitions = 2, 1)) >>> createTopic
          _ <- ZStream.fromIterable(kvs_partition1) >>> publishPartitionTopicKeyValue
          _ <- ZStream.fromIterable(kvs_partition2) >>> publishPartitionTopicKeyValue
          records <- read(tempTopic)
                       .take(10)
                       .runCollect
          kvOut = records
                    .sortBy(_.key)
                    .map(r => PartitionTopicKeyValue(tempTopic, r.partition, r.record.key, r.record.value))
                    .toList
        } yield assert(kvOut)(equalTo(kvs_partition1 ++ kvs_partition2))
      }
    )

}
