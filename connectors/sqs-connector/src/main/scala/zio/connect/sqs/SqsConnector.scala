package zio.connect.sqs

import zio.connect.sqs.SqsConnector.{ReceiveMessage, SendMessage, SendMessageBatch, SqsException}
import zio.prelude.Newtype
import zio.stream.{ZSink, ZStream}
import zio.{Trace, ZIO}

trait SqsConnector {
  def sendMessage(implicit trace: Trace): ZSink[Any, SqsException, SendMessage, SendMessage, Unit]
  def sendMessageBatch(implicit trace: Trace): ZSink[Any, SqsException, SendMessageBatch, SendMessageBatch, Unit]
  def receiveMessages(implicit trace: Trace): ZStream[Any, SqsException, ReceiveMessage]
}

object SqsConnector {
  object QueueUrl extends Newtype[String]
  type QueueUrl = QueueUrl.Type

  object MessageBody extends Newtype[String]
  type MessageBody = MessageBody.Type

  object DelaySeconds extends Newtype[Int]
  type DelaySeconds = DelaySeconds.Type

  object MessageGroupId extends Newtype[String]
  type MessageGroupId = MessageGroupId.Type

  object MessageDeduplicationId extends Newtype[String]
  type MessageDeduplicationId = MessageDeduplicationId.Type

  object MessageId extends Newtype[String]
  type MessageId = MessageId.Type

  case class SqsException(reason: Throwable)

  final case class SendMessage(
    body: MessageBody,
    delaySeconds: Option[DelaySeconds],
    messageGroupId: Option[MessageGroupId],
    messageDeduplicationId: Option[MessageDeduplicationId]
  )

  final case class SendMessageBatch(
    entries: List[SendMessageBatchEntry]
  )

  final case class SendMessageBatchEntry(
    id: MessageId,
    body: MessageBody,
    delaySeconds: Option[DelaySeconds],
    messageGroupId: Option[MessageGroupId],
    messageDeduplicationId: Option[MessageDeduplicationId]
  )

  final case class ReceiveMessage(
    id: MessageId,
    body: MessageBody,
    ack: ZIO[Any, SqsException, Unit]
  )
}
