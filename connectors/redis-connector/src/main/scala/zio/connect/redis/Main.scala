package zio.connect.redis
import zio.redis._
import zio.schema.codec.{Codec, ProtobufCodec}
import zio._

object Main extends ZIOAppDefault {
  val rc = LiveRedisConnector()

  val result: ZIO[Redis, RedisError, Boolean] = rc.set("connectRedis", "Relegating")

  override def run: ZIO[Any with ZIOAppArgs with Scope, Any, Any] =
    result.provide(
      ZLayer.succeed[RedisConfig](RedisConfig.Default),
      RedisExecutor.layer,
      RedisLive.layer,
      ZLayer.succeed[Codec](ProtobufCodec)
    )
}
