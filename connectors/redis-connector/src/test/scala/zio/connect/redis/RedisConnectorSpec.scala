package zio.connect.redis
import zio.Chunk
import zio.connect.redis.RedisConnector._
import zio.stream.ZStream
import zio.test.{ZIOSpecDefault, _}

import java.util.UUID

trait RedisConnectorSpec extends ZIOSpecDefault {

  val redisConnectorSpec = appendKeySpec + delKeySpec

  private lazy val appendKeySpec = {
    suite("append")(
      test("append single key value") {
        val key = genKey
        for {
          _      <- ZStream(Append(key, "a")) >>> append
          result <- ZStream(Get(key)) >>> get
        } yield assertTrue(result == Chunk(GetResult(key, Some("a"))))
      },
      test("append multiple strings") {
        val key = genKey
        for {
          _      <- ZStream(Append(key, "a"), Append(key, "b")) >>> append
          result <- ZStream(Get(key)) >>> get
        } yield assertTrue(result == Chunk(GetResult(key, Some("ab"))))
      }
    )
  }

  private lazy val delKeySpec = {
    suite("del")(
      test("delte an existing value") {
        val key = genKey
        for {
          _      <- ZStream(Append(key, "a")) >>> append
          value  <- ZStream(Get(key)) >>> get
          _      <- ZStream(Del(key), Del(key)) >>> del
          result <- ZStream(Get(key)) >>> get
        } yield assertTrue(result == Chunk(GetResult(key, None)) && value == Chunk(GetResult(key, Some("a"))))
      },
      test("delete an none-existing value") {
        val key: String = genKey
        for {
          result <- ZStream(Get(key)) >>> get
        } yield assertTrue(result == Chunk(GetResult(key, None)))
      }
    )
  }

  def genKey: String = UUID.randomUUID().toString
}
