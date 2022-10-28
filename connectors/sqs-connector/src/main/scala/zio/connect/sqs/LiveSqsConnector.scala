package zio.connect.sqs

import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.model.{SendMessageBatchRequest, SendMessageBatchRequestEntry, SendMessageRequest}
import zio.connect.sqs.SqsConnector.{
  DelaySeconds,
  MessageBody,
  MessageId,
  QueueUrl,
  ReceiveMessage,
  SendMessage,
  SendMessageBatch,
  SqsException
}
import zio.stream.{ZSink, ZStream}
import zio.{Trace, ZIO, ZLayer}

final case class LiveSqsConnector(sqs: AmazonSQS) extends SqsConnector {
  override def sendMessage(implicit trace: Trace): ZSink[Any, SqsException, SendMessage, SendMessage, Unit] =
    ZSink
      .foreach[Any, SqsException, SendMessage] { m =>
        (for {
          request <- ZIO.attempt {
                       val msg = new SendMessageRequest(m.queueUrl.toString, m.body.toString)

                       m.delaySeconds
                         .foreach(delay => msg.withDelaySeconds(DelaySeconds.unwrap(delay)))

                       m.messageGroupId
                         .foreach(groupId => msg.withMessageGroupId(groupId.toString))

                       m.messageDeduplicationId
                         .foreach(deduplicationId => msg.withMessageGroupId(deduplicationId.toString))

                       msg
                     }
          _ <- ZIO.attempt {
                 sqs.sendMessage(request)
               }
        } yield ()).mapError(throwable => SqsException(throwable))
      }

  override def sendMessageBatch(implicit
    trace: Trace
  ): ZSink[Any, SqsException, SendMessageBatch, SendMessageBatch, Unit] =
    ZSink
      .foreach[Any, SqsException, SendMessageBatch] { b =>
        (for {
          entries <- ZIO.foreach(b.entries) { m =>
                       ZIO.attempt {
                         val msg = new SendMessageBatchRequestEntry(m.id.toString, m.body.toString)

                         m.delaySeconds
                           .foreach(delay => msg.withDelaySeconds(DelaySeconds.unwrap(delay)))

                         m.messageGroupId
                           .foreach(groupId => msg.withMessageGroupId(groupId.toString))

                         m.messageDeduplicationId
                           .foreach(deduplicationId => msg.withMessageGroupId(deduplicationId.toString))

                         msg
                       }
                     }
          request <- ZIO.attempt {
                       new SendMessageBatchRequest(b.queueUrl.toString)
                         .withEntries(entries: _*)
                     }
          _ <- ZIO.attempt {
                 sqs.sendMessageBatch(request)
               }
        } yield ()).mapError(throwable => SqsException(throwable))
      }

  override def receiveMessages(
    queueUrl: => QueueUrl
  )(implicit trace: Trace): ZStream[Any, SqsException, ReceiveMessage] =
    ZStream
      .fromIterableZIO(ZIO.attemptBlocking {
        val messageResult = sqs.receiveMessage(queueUrl.toString)
        val messages      = messageResult.getMessages

        val javaIterator = messages.iterator()

        new Iterable[ReceiveMessage] {
          override def iterator: Iterator[ReceiveMessage] = new Iterator[ReceiveMessage] {
            override def hasNext: Boolean = javaIterator.hasNext

            override def next(): ReceiveMessage = {
              val message = javaIterator.next()

              ReceiveMessage(
                MessageId(message.getMessageId),
                MessageBody(message.getBody),
                ZIO.attemptBlocking {
                  sqs.deleteMessage(queueUrl.toString, message.getReceiptHandle)
                }.unit.mapError(throwable => SqsException(throwable))
              )
            }
          }
        }
      })
      .mapError(throwable => SqsException(throwable))
}

object LiveSqsConnector {

  val layer: ZLayer[AmazonSQS, Nothing, LiveSqsConnector] =
    ZLayer.fromZIO(ZIO.service[AmazonSQS].map(LiveSqsConnector(_)))

}
