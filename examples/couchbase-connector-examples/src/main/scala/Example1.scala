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
  val newKey     = DocumentKey("zio-connect-doc")

  val queryObject = QueryObject(bucket, scope, collection, newKey)

  /**
   * As a demonstration of the api this example first checks that the document does not exist, inserts the document
   * unless it was found, checks to see that in fact the document has been inserted, retrieves it, subsequently removes
   * the document and finally, checks again to see if the id/key is no longer present.
   */
  val program: ZIO[CouchbaseConnector, CouchbaseException, String] =
    for {
      keyExists <- ZStream(queryObject) >>> exists
      insertDocument = ZStream(
                         ContentQueryObject(bucket, scope, collection, newKey, Chunk.fromArray(sampleDocument.getBytes))
                       ) >>> insert
      _              <- insertDocument.unless(keyExists)
      documentExists <- ZStream(queryObject) >>> exists
      _ <- ZIO.debug(s"Found at ${LocalDateTime.now(java.time.Clock.systemUTC())}: $documentExists") // should be true
      doc                         <- (get(queryObject) >>> ZPipeline.utf8Decode >>> ZSink.mkString).refineToOrDie[CouchbaseException]
      _                           <- ZStream(queryObject) >>> remove
      documentExistsAfterDeletion <- ZStream(queryObject) >>> exists
      _ <- ZIO.debug(
             s"Found at ${LocalDateTime.now(java.time.Clock.systemUTC())}: $documentExistsAfterDeletion"
           ) // should be false
    } yield doc

  override def run: ZIO[ZIOAppArgs with Scope, Throwable, String] =
    program
      .tap(Console.printLine(_))
      .provide(couchbaseConnectorLiveLayer, cluster)

}
