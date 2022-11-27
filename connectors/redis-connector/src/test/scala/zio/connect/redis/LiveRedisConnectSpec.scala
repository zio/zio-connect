package zio.connect.redis
import zio.redis.{RedisConfig, RedisExecutor, RedisLive}
import zio.schema.codec.{Codec, ProtobufCodec}
import zio.{Scope, ZLayer}
import zio.test.{Spec, TestEnvironment}

object LiveRedisConnectSpec extends RedisConnectorSpec {
  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("LiveRedisConnectSpec")(redisConnectorSpec)
      .provideSomeShared[Scope](
        ZLayer.succeed[RedisConfig](RedisConfig.Default),
        RedisExecutor.layer,
        RedisLive.layer,
        ZLayer.succeed[Codec](ProtobufCodec),
        LiveRedisConnector.layer
      )
}
