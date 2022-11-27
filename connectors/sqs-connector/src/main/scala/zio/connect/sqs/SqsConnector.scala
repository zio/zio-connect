package zio.connect.sqs

import zio.connect.sqs.SqsConnector.{ReceiveMessage, SendMessage, SendMessageBatch, SqsException}
import zio.prelude.Newtype
import zio.stream.{ZSink, ZStream}
import zio.{Trace, ZIO}

trait SqsConnector {
  def receiveMessages(implicit trace: Trace): ZStream[Any, SqsException, ReceiveMessage]
  def sendMessage(implicit trace: Trace): ZSink[Any, SqsException, SendMessage, SendMessage, Unit]
  def sendMessageBatch(implicit trace: Trace): ZSink[Any, SqsException, SendMessageBatch, SendMessageBatch, Unit]
}

object SqsConnector {
  object QueueUrl extends Newtype[String]
  type QueueUrl = QueueUrl.Type

  object MessageBody extends Newtype[String]
  type MessageBody = MessageBody.Type

  object DelaySeconds extends Newtype[Int]
  type DelaySeconds = DelaySeconds.Type

  object MessageId extends Newtype[String]
  type MessageId = MessageId.Type

  case class SqsException(reason: Throwable)

  final case class SendMessage(
    body: MessageBody,
    delaySeconds: Option[DelaySeconds]
  )

  final case class SendMessageBatch(
    entries: List[SendMessageBatchEntry]
  )

  final case class SendMessageBatchEntry(
    id: MessageId,
    body: MessageBody,
    delaySeconds: Option[DelaySeconds]
  )

  final case class ReceiveMessage(
    id: MessageId,
    body: MessageBody,
    ack: ZIO[Any, SqsException, Unit]
  )
}
