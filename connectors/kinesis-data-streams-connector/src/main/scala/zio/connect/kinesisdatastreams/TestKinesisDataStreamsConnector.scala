package zio.connect.kinesisdatastreams

import izumi.reflect.Tag
import zio.connect.kinesisdatastreams.KinesisDataStreamsConnector.{
  KinesisDataStreamsException,
  PartitionKey,
  ProducerRecord
}
import zio.connect.kinesisdatastreams.TestKinesisDataStreamsConnector.TestKinesisDataStream
import zio.stm.{STM, TRef, ZSTM}
import zio.stream.ZSink
import zio.{Chunk, Trace, ZIO, ZLayer}

private[kinesisdatastreams] final case class TestKinesisDataStreamsConnector[T](
  kinesisDataStream: TestKinesisDataStream[T]
) extends KinesisDataStreamsConnector[T] {

  override def sinkChunked(implicit
    trace: Trace
  ): ZSink[Any, KinesisDataStreamsException, Chunk[ProducerRecord[T]], Nothing, Unit] =
    ZSink.foreach(records => kinesisDataStream.write(records))

}

object TestKinesisDataStreamsConnector {

  def layer[T](implicit tag: Tag[T]): ZLayer[Any, Nothing, TestKinesisDataStreamsConnector[T]] =
    ZLayer.fromZIO(STM.atomically {
      for {
        a <- TRef.make(Map.empty[PartitionKey, Chunk[ProducerRecord[T]]])
      } yield TestKinesisDataStreamsConnector(TestKinesisDataStream[T](a))
    })

  private[kinesisdatastreams] final case class TestKinesisDataStream[T](
    repo: TRef[Map[PartitionKey, Chunk[ProducerRecord[T]]]]
  ) {
    def write(records: Chunk[ProducerRecord[T]])(implicit
      trace: Trace
    ): ZIO[Any, Nothing, Unit] =
      ZSTM.atomically {
        for {
          _ <- ZSTM.foreach(records) { record =>
                 repo.update(r =>
                   r.updated(
                     record.partitionKey,
                     r.getOrElse(record.partitionKey, Chunk.empty[ProducerRecord[T]]).appended(record)
                   )
                 )
               }
        } yield ()
      }
  }

}
