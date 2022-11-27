package zio.connect.redis

import zio.connect.redis.RedisConnector.{Append, Del, Get}
import zio.redis._
import zio.schema.Schema
import zio.stream.ZSink
import zio.{Trace, ZIO, ZLayer}

trait RedisConnector {
  def append[K: Schema, V: Schema](implicit trace: Trace): ZSink[Any, RedisError, Append[K, V], Append[K, V], Set[Long]]
  def del[K: Schema](implicit trace: Trace): ZSink[Any, RedisError, Del[K], Del[K], Set[Long]]
  def get[K: Schema](implicit trace: Trace): ZSink[Any, RedisError, Get[K], Get[K], Set[Option[String]]]

}

object RedisConnector {
  final case class Append[K, V](key: K, value: V)
  final case class Del[K](key: K, keys: K*)
  final case class Get[K](key: K)
}

case class LiveRedisConnector(redis: Redis) extends RedisConnector {
  override def append[K: Schema, V: Schema](implicit
    trace: Trace
  ): ZSink[Any, RedisError, Append[K, V], Append[K, V], Set[Long]] =
    ZSink.foldLeftZIO[Any, RedisError, Append[K, V], Set[Long]](Set.empty[Long])((s, a) =>
      zio.redis.append(a.key, a.value).provide(ZLayer.succeed(redis)).map(a => s + a)
    )
  override def del[K: Schema](implicit
    trace: Trace
  ): ZSink[Any, RedisError, Del[K], Del[K], Set[Long]] =
    ZSink.foldLeftZIO[Any, RedisError, Del[K], Set[Long]](Set.empty[Long])((s, a) =>
      zio.redis.del(a.key).provide(ZLayer.succeed(redis)).map(a => s + a)
    )
  override def get[K: Schema](implicit
    trace: Trace
  ): ZSink[Any, RedisError, Get[K], Get[K], Set[Option[String]]] =
    ZSink.foldLeftZIO[Any, RedisError, Get[K], Set[Option[String]]](Set.empty[Option[String]]) { (s, a) =>
      zio.redis
        .get(a.key)
        .returning[String]
        .provide(ZLayer.succeed(redis))
        .map(a => s + a)
    }
}

object LiveRedisConnector {
  val layer: ZLayer[Redis, Nothing, LiveRedisConnector] =
    ZLayer.fromZIO(ZIO.service[Redis].map(LiveRedisConnector(_)))
}
