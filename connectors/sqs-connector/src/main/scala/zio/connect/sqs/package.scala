package zio.connect

import zio.Trace
import zio.connect.sqs.SqsConnector.{ReceiveMessage, SendMessage, SendMessageBatch, SqsException}
import zio.stream.{ZSink, ZStream}

package object sqs {

  def sendMessage(implicit trace: Trace): ZSink[SqsConnector, SqsException, SendMessage, SendMessage, Unit] =
    ZSink.serviceWithSink(_.sendMessage)

  def sendMessageBatch(implicit
    trace: Trace
  ): ZSink[SqsConnector, SqsException, SendMessageBatch, SendMessageBatch, Unit] =
    ZSink.serviceWithSink(_.sendMessageBatch)

  def receiveMessages(implicit
    trace: Trace
  ): ZStream[SqsConnector, SqsException, ReceiveMessage] =
    ZStream.serviceWithStream(_.receiveMessages)
}
