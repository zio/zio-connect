package zio.connect.kinesisdatastreams

import zio.connect.kinesisdatastreams.KinesisDataStreamsConnector.{KinesisDataStreamsException, ProducerRecord}
import zio.prelude.Subtype
import zio.stream.ZSink
import zio.{Chunk, Trace}

trait KinesisDataStreamsConnector[T] {

  def sinkChunked(implicit
    trace: Trace
  ): ZSink[Any, KinesisDataStreamsException, Chunk[ProducerRecord[T]], Nothing, Unit]
}

object KinesisDataStreamsConnector {

  object StreamName extends Subtype[String]
  type StreamName = StreamName.Type

  object PartitionKey extends Subtype[String]
  type PartitionKey = PartitionKey.Type

  final case class ProducerRecord[T](partitionKey: PartitionKey, data: T)

  case class KinesisDataStreamsException(reason: Throwable)

}
