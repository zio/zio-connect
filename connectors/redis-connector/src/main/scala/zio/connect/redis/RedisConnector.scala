package zio.connect.redis

import zio.connect.redis.RedisConnector._
import zio.redis._
import zio.schema.Schema
import zio.stream.ZSink
import zio.{Chunk, Trace, ZIO, ZLayer}

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

case class LiveRedisConnector(redis: Redis) extends RedisConnector {
  override def append[K: Schema, V: Schema](implicit
    trace: Trace
  ): ZSink[Any, RedisError, Append[K, V], Nothing, Unit] =
    ZSink.foreach(a => zio.redis.append(a.key, a.value).provide(ZLayer.succeed(redis)))

  override def del[K: Schema](implicit
    trace: Trace
  ): ZSink[Any, RedisError, Del[K], Nothing, Unit] =
    ZSink
      .foreach(a => zio.redis.del(a.key).provide(ZLayer.succeed(redis)))

  override def get[K: Schema](implicit
    trace: Trace
  ): ZSink[Any, RedisError, Get[K], Nothing, Chunk[GetResult[K]]] =
    ZSink
      .foldLeftZIO(Chunk.empty[GetResult[K]]) { (s, a: Get[K]) =>
        zio.redis
          .get[K](a.key)
          .returning[String]
          .provide(ZLayer.succeed(redis))
          .map(as => s ++ Chunk(GetResult(a.key, as)))
      }
      .ignoreLeftover
}

object LiveRedisConnector {
  val layer: ZLayer[Redis, Nothing, LiveRedisConnector] =
    ZLayer.fromZIO(ZIO.service[Redis].map(LiveRedisConnector(_)))
}
