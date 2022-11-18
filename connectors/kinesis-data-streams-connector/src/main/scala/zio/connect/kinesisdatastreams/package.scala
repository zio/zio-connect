package zio.connect

import izumi.reflect.Tag
import nl.vroste.zio.kinesis.client.Producer
import zio.connect.kinesisdatastreams.KinesisDataStreamsConnector.{KinesisDataStreamsException, ProducerRecord}
import zio.stream.ZSink
import zio.{Chunk, Trace, ZLayer}

package object kinesisdatastreams {

  def sinkChunked[T](implicit
    trace: Trace,
    tag: Tag[T]
  ): ZSink[KinesisDataStreamsConnector[T], KinesisDataStreamsException, Chunk[ProducerRecord[T]], Nothing, Unit] =
    ZSink.serviceWithSink(_.sinkChunked)

  def kinesisDataStreamsConnectorLiveLayer[T]: ZLayer[Producer[T], Nothing, LiveKinesisDataStreamsConnector[T]] =
    LiveKinesisDataStreamsConnector.layer[T]

  def kinesisDataStreamsConnectorTestLayer[T]: ZLayer[Any, Nothing, TestKinesisDataStreamsConnector[T]] =
    TestKinesisDataStreamsConnector.layer[T]

}
