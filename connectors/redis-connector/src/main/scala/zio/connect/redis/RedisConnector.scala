package zio.connect.redis

import zio.connect.redis.RedisConnector._
import zio.redis._
import zio.schema.Schema
import zio.stream.ZSink
import zio.{Chunk, Trace}

trait RedisConnector {
  def append[K: Schema, V: Schema](implicit trace: Trace): ZSink[Any, RedisError, Append[K, V], Nothing, Unit]
  def del[K: Schema](implicit trace: Trace): ZSink[Any, RedisError, Del[K], Nothing, Unit]
  def get[K: Schema](implicit trace: Trace): ZSink[Any, RedisError, Get[K], Nothing, Chunk[GetResult[K]]]

}

object RedisConnector {
  final case class Append[K, V](key: K, value: V)
  final case class Del[K](key: K, keys: K*)
  final case class Get[K](key: K, keys: K*)
  final case class GetResult[K](key: K, value: Option[String])
}
