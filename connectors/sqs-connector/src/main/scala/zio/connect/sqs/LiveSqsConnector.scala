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

final case class LiveSqsConnector(sqs: AmazonSQS, queueUrl: QueueUrl) extends SqsConnector {
  override def sendMessage(implicit trace: Trace): ZSink[Any, SqsException, SendMessage, SendMessage, Unit] =
    ZSink
      .foreach[Any, SqsException, SendMessage] { m =>
        (for {
          request <- ZIO.attempt {
                       val msg = new SendMessageRequest(queueUrl.toString, m.body.toString)

                       m.delaySeconds
                         .foreach(delay => msg.withDelaySeconds(DelaySeconds.unwrap(delay)))

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

                         msg
                       }
                     }
          request <- ZIO.attempt {
                       new SendMessageBatchRequest(queueUrl.toString)
                         .withEntries(entries: _*)
                     }
          _ <- ZIO.attempt {
                 sqs.sendMessageBatch(request)
               }
        } yield ()).mapError(throwable => SqsException(throwable))
      }

  override def receiveMessages(implicit trace: Trace): ZStream[Any, SqsException, ReceiveMessage] =
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

  val layer: ZLayer[AmazonSQS with QueueUrl, Nothing, LiveSqsConnector] = {
    ZLayer.fromZIO(for {
      amazonSqs <- ZIO.service[AmazonSQS]
      queue     <- ZIO.service[QueueUrl]
    } yield LiveSqsConnector(amazonSqs, queue))
  }

}
