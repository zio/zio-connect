package zio.connect

import izumi.reflect.Tag
import zio.connect.kinesisdatastreams.KinesisDataStreamsConnector.{KinesisDataStreamsException, ProducerRecord}
import zio.stream.ZSink
import zio.{Chunk, Trace}

package object kinesisdatastreams {
  def sinkChunked[T](implicit
    trace: Trace,
    tag: Tag[T]
  ): ZSink[KinesisDataStreamsConnector[T], KinesisDataStreamsException, Chunk[ProducerRecord[T]], Nothing, Unit] =
    ZSink.serviceWithSink(_.sinkChunked)

}
