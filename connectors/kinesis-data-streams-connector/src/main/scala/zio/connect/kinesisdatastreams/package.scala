package zio.connect

import izumi.reflect.Tag
import nl.vroste.zio.kinesis.client.Producer
import zio.connect.kinesisdatastreams.KinesisDataStreamsConnector.{KinesisDataStreamsException, ProducerRecord}
import zio.stream.ZSink
import zio.{Trace, ZLayer}

package object kinesisdatastreams {

  def sink[T](implicit
    trace: Trace,
    tag: Tag[T]
  ): ZSink[KinesisDataStreamsConnector[T], KinesisDataStreamsException, ProducerRecord[T], Nothing, Unit] =
    ZSink.serviceWithSink(_.sink)

  def kinesisDataStreamsConnectorLiveLayer[T](implicit
    tag: Tag[T]
  ): ZLayer[Producer[T], Nothing, LiveKinesisDataStreamsConnector[T]] =
    LiveKinesisDataStreamsConnector.layer[T]

  def kinesisDataStreamsConnectorTestLayer[T](implicit
    tag: Tag[T]
  ): ZLayer[Any, Nothing, TestKinesisDataStreamsConnector[T]] =
    TestKinesisDataStreamsConnector.layer[T]

}
