package zio.connect.kafka

import zio.Trace
import zio.stream.ZStream

import java.io.IOException

trait KafkaConnector {
  def subscribe(topic: => String)(implicit trace: Trace): ZStream[Any, IOException, Byte]
}

object KafkaConnector {

  def subscribe(topic: => String): ZStream[KafkaConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.subscribe(topic))

}
