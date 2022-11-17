package zio.connect.kinesisdatastreams

import zio.connect.kinesisdatastreams.KinesisDataStreamsConnector.{
  KinesisDataStreamsException,
  PartitionKey,
  ProducerRecord
}
import zio.stream.ZStream
import zio.test._
import zio.Chunk

trait KinesisDataStreamsConnectorSpec extends ZIOSpecDefault {

  val kinesisDataStreamsConnectorSpec = produceRecordSuite

  private lazy val produceRecordSuite: Spec[KinesisDataStreamsConnector[String], KinesisDataStreamsException] =
    suite("produceRecords")(
      test("kinesis stream is populated by partition key ") {
        val record1 = ProducerRecord(PartitionKey("1"), "Data1")
        val record2 = ProducerRecord(PartitionKey("1"), "Data2")
        val record3 = ProducerRecord(PartitionKey("2"), "Data2")
        val input   = Chunk(record1, record2, record3)
        for {
          _ <- ZStream(input) >>> sinkChunked[String]
        } yield assertCompletes
      }
    )
}
