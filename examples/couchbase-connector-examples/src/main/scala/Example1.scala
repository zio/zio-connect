import com.couchbase.client.java.Cluster
import zio._
import zio.connect.couchbase.CouchbaseConnector._
import zio.connect.couchbase._
import zio.stream._

import java.time.LocalDateTime

/**
 * This example assumes you have a Couchbase cluster running with port 11210 available on localhost
 * and the "gamesim-sample" bucket created.
 */
object Example1 extends ZIOAppDefault {

  val sampleDocument =
    """
      |{
      |  "experience": 55,
      |  "hitpoints": 10,
      |  "jsonType": "player",
      |  "level": 2,
      |  "loggedIn": true,
      |  "name": "ZIO",
      |  "uuid": "edc5aedf-9cb6-4c11-90d4-9645083053e8"
      |  }
      |""".stripMargin

  val cluster = ZLayer.scoped(
    ZIO.attempt(
      Cluster
        .connect("127.0.0.1", "admin", "admin22")
    )
  )

  // Couchbase primitives are modeled as zio prelude newtypes
  val bucket     = BucketName("gamesim-sample")
  val collection = CollectionName("_default")
  val scope      = ScopeName("_default")
  val newKey     = DocumentKey("zio-connect-doc")

  val queryObject = QueryObject(bucket, scope, collection, newKey)

  /**
   * As a demonstration of the api this example first checks that the document does not exist, inserts the document
   * unless it was found, checks to see that in fact the document has been inserted, retrieves it, subsequently removes
   * the document and finally, checks again to see if the id/key is no longer present.
   */
  val program: ZIO[CouchbaseConnector, Throwable, String] =
    for {
      keyExists <- ZStream(queryObject).run(exists).mapError(_.reason)
      _ <- ZStream(
             ContentQueryObject(bucket, scope, collection, newKey, Chunk.fromArray(sampleDocument.getBytes))
           ).run(insert).mapError(_.reason).unless(keyExists)
      _ <- ZStream(queryObject)
             .run(exists)
             .mapError(_.reason)
             .debug(s"Found ${LocalDateTime.now(java.time.Clock.systemUTC())}: ") // should be true
      doc <- get(queryObject)
               .mapError(_.reason)
               .run(ZPipeline.utf8Decode >>> ZSink.mkString)
      _ <- ZStream(queryObject).run(remove).mapError(_.reason)
      _ <- ZStream(queryObject)
             .run(exists)
             .mapError(_.reason)
             .debug(s"Found ${LocalDateTime.now(java.time.Clock.systemUTC())}: ") // should be false
    } yield doc

  override def run: ZIO[Any with ZIOAppArgs with Scope, Any, Any] =
    program
      .tap(Console.printLine(_))
      .provide(couchbaseConnectorLiveLayer, cluster)

}
