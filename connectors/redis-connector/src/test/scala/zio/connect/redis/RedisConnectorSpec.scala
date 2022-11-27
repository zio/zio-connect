package zio.connect.redis
import zio.connect.redis.RedisConnector._
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test.Gen.uuid
import zio.test.{ZIOSpecDefault, _}

trait RedisConnectorSpec extends ZIOSpecDefault {

  val redisConnectorSpec = appendKeySpec + delKeySpec

  private lazy val appendKeySpec = {
    suite("append")(
      test("append multiple strings") {
        val key: String = uuid.toString
        for {
          result <- ZStream(Append(key, "a"), Append(key, "b")) >>> append
        } yield assert(result == Set(1, 2))(isTrue)
      }
    )
  }

  private lazy val delKeySpec = {
    suite("del")(
      test("delete an existing value") {
        val key: String = "keyToDel"
        for {
          _      <- ZStream(Append(key, "a")) >>> append
          _      <- ZStream(Del(key)) >>> del
          result <- ZStream(Get(key)) >>> get
        } yield assert(result == Set(None))(isTrue)
      }
    )
  }

}
