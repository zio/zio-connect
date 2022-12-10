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
      test("append multiple strings") {
        val key: String = UUID.randomUUID().toString
        for {
          result <- ZStream(Append(key, "a"), Append(key, "b")) >>> append
        } yield assertTrue(result == ())
      }
    )
  }

  private lazy val delKeySpec = {
    suite("del")(
      test("delete an existing value") {
        val key: String  = "keyToDel"
        val key1: String = "keyToDel1"
        (for {
          _      <- ZStream(Append(key, "a")) >>> append
          _      <- ZStream(Append(key1, "a")) >>> append
          _      <- ZStream(Del(key), Del(key1)) >>> del
          result <- ZStream(Get(key)) >>> get
        } yield assertTrue(result == Chunk(GetResult(key, None)))).debug("result: ")
      }
    )
  }

}
