package zio.connect.kinesisdatastreams
import izumi.reflect.Tag
import zio.connect.kinesisdatastreams.KinesisDataStreamsConnector.{KinesisDataStreamsException, ProducerRecord}
import zio.stream.ZSink
import zio.{Chunk, Trace, ZIO, ZLayer}
import nl.vroste.zio.kinesis.client.Producer

final case class LiveKinesisDataStreamsConnector[T](producer: Producer[T]) extends KinesisDataStreamsConnector[T] {
  override def sinkChunked(implicit
    trace: Trace
  ): ZSink[Any, KinesisDataStreamsException, Chunk[KinesisDataStreamsConnector.ProducerRecord[T]], Nothing, Unit] =
    producer.sinkChunked
      .mapError(e => KinesisDataStreamsException.apply(e))
      .contramap[Chunk[ProducerRecord[T]]](chunk =>
        chunk.map(record => nl.vroste.zio.kinesis.client.ProducerRecord[T](record.partitionKey.toString, record.data))
      )
}

object LiveKinesisDataStreamsConnector {

  def layer[T](implicit tag: Tag[T]): ZLayer[Producer[T], Nothing, LiveKinesisDataStreamsConnector[T]] =
    ZLayer.fromZIO(ZIO.service[Producer[T]].map(prod => LiveKinesisDataStreamsConnector[T](prod)))

}
