package zio.connect.redis
import zio.redis._
import zio.{ULayer, ZLayer}

trait RedisConnector
    extends api.Connection
    with api.Geo
    with api.Hashes
    with api.HyperLogLog
    with api.Keys
    with api.Lists
    with api.Sets
    with api.Strings
    with api.SortedSets
    with api.Streams
    with api.Scripting
    with options.Connection
    with options.Geo
    with options.Keys
    with options.Shared
    with options.SortedSets
    with options.Strings
    with options.Lists
    with options.Streams
    with options.Scripting

final case class LiveRedisConnector() extends RedisConnector

object LiveRedisConnector {
  val layer: ULayer[LiveRedisConnector.type] =
    ZLayer.succeed(LiveRedisConnector)
}
