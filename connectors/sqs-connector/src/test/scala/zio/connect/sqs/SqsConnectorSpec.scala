package zio.connect.sqs

import zio.connect.sqs.SqsConnector._
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._
import zio.{Chunk, ZIO}

trait SqsConnectorSpec extends ZIOSpecDefault {

  val sqsConnectorSpec =
    sendMessageSpec + sendMessageBatchSpec

  private lazy val sendMessageSpec = suite("sendMessage")(
    test("succeeds") {
      val sendMessageInput = SendMessage(
        MessageBody(
          "msgbody"
        ),
        None
      )
      for {
        _                <- ZStream(sendMessageInput) >>> sendMessage
        receivedMessages <- receiveMessages.runCollect
        _                <- ZIO.foreach(receivedMessages)(_.ack)
      } yield assert(receivedMessages)(hasSize(equalTo(1))) &&
        assert(receivedMessages.map(_.body))(equalTo(Chunk(sendMessageInput.body)))
    }
  )
  private lazy val sendMessageBatchSpec = suite("sendMessageBatch")(
    test("succeeds in sending single message") {
      val batchEntry = SendMessageBatchEntry(
        MessageId("1"),
        MessageBody("body1"),
        None
      )
      val sendMessageBatchInput = SendMessageBatch(List(batchEntry))
      for {
        _                <- ZStream(sendMessageBatchInput) >>> sendMessageBatch
        receivedMessages <- receiveMessages.runCollect
        _                <- ZIO.foreach(receivedMessages)(_.ack)
      } yield assert(receivedMessages)(hasSize(equalTo(1))) &&
        assert(receivedMessages.map(_.body))(equalTo(Chunk(batchEntry.body)))
    },
    test("succeeds in sending two messages") {
      val batchEntry1 = SendMessageBatchEntry(
        MessageId("1"),
        MessageBody("body1"),
        None
      )
      val batchEntry2 = SendMessageBatchEntry(
        MessageId("2"),
        MessageBody("body2"),
        None
      )
      val sendMessageBatchInput = SendMessageBatch(List(batchEntry1, batchEntry2))
      for {
        _                 <- ZStream(sendMessageBatchInput) >>> sendMessageBatch
        receivedMessages1 <- receiveMessages.runCollect
        _                 <- ZIO.foreach(receivedMessages1)(_.ack)
        receivedMessages2 <- receiveMessages.runCollect
        _                 <- ZIO.foreach(receivedMessages2)(_.ack)
        receivedMessages   = receivedMessages1 ++ receivedMessages2
      } yield assert(receivedMessages1)(hasSize(equalTo(1))) &&
        assert(receivedMessages2)(hasSize(equalTo(1))) &&
        assert(receivedMessages.map(_.body))(equalTo(Chunk(batchEntry1.body, batchEntry2.body)))
    }
  )
}
