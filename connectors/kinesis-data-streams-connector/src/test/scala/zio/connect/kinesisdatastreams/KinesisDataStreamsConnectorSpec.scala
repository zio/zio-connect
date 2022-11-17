package zio.connect.kinesisdatastreams

import zio.connect.kinesisdatastreams.KinesisDataStreamsConnector.{
  KinesisDataStreamsException,
  PartitionKey,
  ProducerRecord
}
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._
import zio.Chunk

trait KinesisDataStreamsConnectorSpec extends ZIOSpecDefault {

  val kinesisDataStreamsConnectorSpec = produceRecordSuite

  private lazy val produceRecordSuite: Spec[KinesisDataStreamsConnector[String], KinesisDataStreamsException] =
    suite("produceRecords")(
      test("kinesis stream is populated by partition key ") {
//        val streamName = StreamName(UUID.randomUUID().toString)
        val record1 = ProducerRecord(PartitionKey("1"), "Data1")
        val record2 = ProducerRecord(PartitionKey("1"), "Data2")
        val record3 = ProducerRecord(PartitionKey("2"), "Data2")
        for {
          result <- ZStream.succeed(Chunk(record1, record2, record3)).run(sinkChunked[String])
        } yield assert(result)(equalTo(result))
      }
    )
}
