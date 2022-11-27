package zio.connect.sqs

import zio.connect.sqs.SqsConnector.{
  MessageId,
  ReceiveMessage,
  SendMessage,
  SendMessageBatch,
  SendMessageBatchEntry,
  SqsException
}
import zio.connect.sqs.TestSqsConnector.TestQueueMessage
import zio.stream.{ZSink, ZStream}
import zio.{Chunk, Queue, Ref, Trace, ZIO, ZLayer}

/*
  TODO:
 * Implement wait based on delaySeconds
 */
private[sqs] final case class TestSqsConnector(sqs: Queue[TestQueueMessage]) extends SqsConnector {

  override def sendMessage(implicit trace: Trace): ZSink[Any, SqsException, SendMessage, SendMessage, Unit] =
    ZSink
      .fromQueue(sqs)
      .contramapZIO(sendMessage =>
        Ref
          .make(false)
          .map(ack =>
            TestQueueMessage(
              SendMessageBatchEntry(
                MessageId("testId"),
                sendMessage.body,
                sendMessage.delaySeconds
              ),
              ack
            )
          )
      )

  override def sendMessageBatch(implicit
    trace: Trace
  ): ZSink[Any, SqsException, SendMessageBatch, SendMessageBatch, Unit] =
    ZSink
      .fromQueue(sqs)
      .contramapChunksZIO { (batchChunk: Chunk[SendMessageBatch]) =>
        val entriesChunk = batchChunk.flatMap(batch =>
          Chunk(
            batch.entries.map(batchEntry =>
              SendMessageBatchEntry(
                MessageId("testId"),
                batchEntry.body,
                batchEntry.delaySeconds
              )
            ): _*
          )
        )
        ZIO.foreach(entriesChunk)(entry => Ref.make(false).map(TestQueueMessage(entry, _)))
      }

  override def receiveMessages(implicit trace: Trace): ZStream[Any, SqsException, ReceiveMessage] =
    ZStream
      .fromZIO(sqs.take)
      .filterZIO(_.ack.get.map(!_))
      .mapZIO(testMsg => sqs.offer(testMsg).as(testMsg))
      .map(testMsg =>
        ReceiveMessage(
          testMsg.message.id,
          testMsg.message.body,
          testMsg.ack.set(true)
        )
      )
}

object TestSqsConnector {
  private[sqs] final case class TestQueueMessage(
    message: SendMessageBatchEntry,
    ack: Ref[Boolean]
  )

  val layer: ZLayer[Any, Nothing, TestSqsConnector] = {
    ZLayer.fromZIO(Queue.unbounded[TestQueueMessage].map(TestSqsConnector(_)))
  }
}
