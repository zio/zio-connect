package zio.connect.kinesisdatastreams
import izumi.reflect.Tag
import zio.connect.kinesisdatastreams.KinesisDataStreamsConnector.{KinesisDataStreamsException, ProducerRecord}
import zio.stream.ZSink
import zio.{Trace, ZIO, ZLayer}
import nl.vroste.zio.kinesis.client.Producer

final case class LiveKinesisDataStreamsConnector[T](producer: Producer[T]) extends KinesisDataStreamsConnector[T] {
  override def sink(implicit
    trace: Trace
  ): ZSink[Any, KinesisDataStreamsException, KinesisDataStreamsConnector.ProducerRecord[T], Nothing, Unit] =
    producer.sink
      .contramap[ProducerRecord[T]](record =>
        nl.vroste.zio.kinesis.client.ProducerRecord[T](record.partitionKey, record.data)
      )
      .mapError(e => KinesisDataStreamsException.apply(e))
}

object LiveKinesisDataStreamsConnector {

  def layer[T](implicit tag: Tag[T]): ZLayer[Producer[T], Nothing, LiveKinesisDataStreamsConnector[T]] =
    ZLayer.fromZIO(ZIO.service[Producer[T]].map(prod => LiveKinesisDataStreamsConnector[T](prod)))

}
