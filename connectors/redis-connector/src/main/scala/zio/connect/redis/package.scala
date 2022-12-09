package zio.connect
import zio.{Chunk, Trace}
import zio.connect.redis.RedisConnector._
import zio.redis.RedisError
import zio.stream.ZSink

package object redis {
  def append(implicit trace: Trace): ZSink[RedisConnector, Any, Append[String, String], Nothing, Unit] =
    ZSink.serviceWithSink(_.append)

  def del(implicit trace: Trace): ZSink[RedisConnector, RedisError, Del[String], Nothing, Unit] =
    ZSink.serviceWithSink(_.del)

  def get(implicit trace: Trace): ZSink[RedisConnector, Any, Get[String], Nothing, Chunk[GetResult[String]]] =
    ZSink.serviceWithSink(_.get)
}
