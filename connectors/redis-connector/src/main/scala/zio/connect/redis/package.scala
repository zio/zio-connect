package zio.connect
import zio.Trace
import zio.connect.redis.RedisConnector._
import zio.stream.ZSink

package object redis {
  def append(implicit trace: Trace): ZSink[LiveRedisConnector, Any, Append[String, String], Any, Set[Long]] =
    ZSink.serviceWithSink(_.append)

  def del(implicit trace: Trace): ZSink[LiveRedisConnector, Any, Del[String], Any, Set[Long]] =
    ZSink.serviceWithSink(_.del)

  def get(implicit trace: Trace): ZSink[LiveRedisConnector, Any, Get[String], Any, Set[Option[String]]] =
    ZSink.serviceWithSink(_.get)
}
