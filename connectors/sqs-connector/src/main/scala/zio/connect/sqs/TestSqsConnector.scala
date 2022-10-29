package zio.connect.sqs

import zio.connect.sqs.SqsConnector.{
  MessageId,
  ReceiveMessage,
  SendMessage,
  SendMessageBatch,
  SendMessageBatchEntry,
  SqsException
}
import zio.stream.{ZSink, ZStream}
import zio.{Chunk, Queue, Trace, ZIO, ZLayer}

/*
  TODO:
 * Make receiveMessages not drop instantly from the queue and only when ack is called
 * Implement wait based on delaySeconds
 */
private[sqs] final case class TestSqsConnector(sqs: Queue[SendMessageBatchEntry]) extends SqsConnector {
  override def sendMessage(implicit trace: Trace): ZSink[Any, SqsException, SendMessage, SendMessage, Unit] =
    ZSink
      .fromQueue(sqs)
      .contramap(sendMessage =>
        SendMessageBatchEntry(
          MessageId("testId"),
          sendMessage.body,
          sendMessage.delaySeconds
        )
      )

  override def sendMessageBatch(implicit
    trace: Trace
  ): ZSink[Any, SqsException, SendMessageBatch, SendMessageBatch, Unit] =
    ZSink
      .fromQueue(sqs)
      .contramapChunks[SendMessageBatch](batchChunk =>
        batchChunk.flatMap(batch =>
          Chunk(
            batch.entries.map(batchEntry =>
              SendMessageBatchEntry(
                batchEntry.id,
                batchEntry.body,
                batchEntry.delaySeconds
              )
            ): _*
          )
        )
      )

  override def receiveMessages(implicit trace: Trace): ZStream[Any, SqsException, ReceiveMessage] =
    ZStream
      .fromQueue(sqs)
      .map(batchEntry =>
        ReceiveMessage(
          batchEntry.id,
          batchEntry.body,
          ZIO.unit
        )
      )
}

object TestSqsConnector {
  val layer: ZLayer[Any, Nothing, TestSqsConnector] = {
    ZLayer.fromZIO(Queue.unbounded[SendMessageBatchEntry].map(TestSqsConnector(_)))
  }
}
