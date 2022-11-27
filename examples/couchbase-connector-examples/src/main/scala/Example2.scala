import com.couchbase.client.java.Cluster
import zio._
import zio.connect.couchbase.CouchbaseConnector._
import zio.connect.couchbase._
import zio.json._
import zio.stream._

/**
 * This example assumes you have a Couchbase cluster running with port 11210 available on localhost
 * and the "gamesim-sample" bucket created.
 */
object Example2 extends ZIOAppDefault {

  // This data is sample data available from Couchbase's sample buckets
  // (https://docs.couchbase.com/server/current/manage/manage-settings/install-sample-buckets.html#scopes-collection-and-sample-buckets)
  case class GameSim(
    experience: Int,
    hitpoints: Int,
    jsonType: String,
    level: Int,
    loggedIn: Boolean,
    name: String,
    uuid: String
  )

  implicit val encoder = DeriveJsonEncoder.gen[GameSim]
  implicit val decoder = DeriveJsonDecoder.gen[GameSim]

  val cluster = ZLayer.scoped(
    ZIO.acquireRelease(
      ZIO.attempt(
        Cluster
          .connect("127.0.0.1", "admin", "admin22")
      )
    )(c => ZIO.attempt(c.disconnect()).orDie)
  )

  // Couchbase primitives are modeled as zio prelude newtypes
  val bucket     = BucketName("gamesim-sample")
  val collection = CollectionName("_default")
  val scope      = ScopeName("_default")
  val key        = DocumentKey("Aaron0")

  val queryObject = QueryObject(bucket, scope, collection, key)

  /*
   * In this example, we obtain a "gamesim" document from the Couchbase cluster and update the `hitpoints` field
   * with the help of zio-json. We then upsert the document back into the cluster. Retrieve it again and verify the hitpoints
   * has been incremented
   */
  val program: ZIO[CouchbaseConnector, CouchbaseException, String] =
    for {
      doc     <- (get(queryObject) >>> ZPipeline.utf8Decode >>> ZSink.mkString).refineToOrDie[CouchbaseException]
      gameSim <- docFromJson(doc)
      update   = gameSim.copy(hitpoints = gameSim.hitpoints + 1)
      _ <-
        ZStream(ContentQueryObject(bucket, scope, collection, key, Chunk.fromArray(update.toJson.getBytes))) >>> upsert
      updated <- (get(queryObject) >>> ZPipeline.utf8Decode >>> ZSink.mkString)
                   .refineToOrDie[CouchbaseException]
                   .flatMap(json => docFromJson(json))
    } yield s"Updated hit points (${updated.hitpoints}) - Original hit points (${gameSim.hitpoints}) == 1 should be ${updated.hitpoints == gameSim.hitpoints + 1}"

  override def run: ZIO[ZIOAppArgs with Scope, Throwable, String] =
    program
      .tap(Console.printLine(_))
      .provide(couchbaseConnectorLiveLayer, cluster)

  private def docFromJson[A](json: String)(implicit decoder: JsonDecoder[A]): ZIO[Any, Nothing, A] =
    ZIO.fromEither(json.fromJson[A]).mapError(new RuntimeException(_)).orDie
}
