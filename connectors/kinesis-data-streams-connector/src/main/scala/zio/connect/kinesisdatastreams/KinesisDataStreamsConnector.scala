package zio.connect.kinesisdatastreams

import zio.connect.kinesisdatastreams.KinesisDataStreamsConnector.{KinesisDataStreamsException, ProducerRecord}
import zio.prelude.Newtype
import zio.stream.ZSink
import zio.{Chunk, Trace}

trait KinesisDataStreamsConnector[T] {

  def sinkChunked(implicit
    trace: Trace
  ): ZSink[Any, KinesisDataStreamsException, Chunk[ProducerRecord[T]], Nothing, Unit]
}

object KinesisDataStreamsConnector {

  object StreamName extends Newtype[String]
  type StreamName = StreamName.Type

  object PartitionKey extends Newtype[String]
  type PartitionKey = PartitionKey.Type

  final case class ProducerRecord[T](partitionKey: PartitionKey, data: T)

  case class KinesisDataStreamsException(reason: Throwable)

}
