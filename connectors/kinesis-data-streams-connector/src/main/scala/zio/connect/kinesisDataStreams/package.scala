package zio.connect

import izumi.reflect.Tag
import zio.connect.kinesisDataStreams.KinesisDataStreamsConnector.{KinesisDataStreamsException, ProducerRecord}
import zio.stream.ZSink
import zio.{Chunk, Trace}

package object kinesisDataStreams {
  def sinkChunked[T]()(implicit
    trace: Trace,
    tag: Tag[T]
  ): ZSink[KinesisDataStreamsConnector[T], KinesisDataStreamsException, Chunk[ProducerRecord[T]], Nothing, Unit] =
    ZSink.serviceWithSink(_.sinkChunked())

}
