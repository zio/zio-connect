package zio.connect.couchbase

import zio.connect.couchbase.CouchbaseConnector._
import zio.stream.ZStream
import zio.test.Assertion.equalTo
import zio.test._
import zio.{Chunk, ZIO}

import java.util.UUID

trait CouchbaseConnectorSpec extends ZIOSpecDefault {

  val couchbaseConnectorSpec: Spec[CouchbaseConnector, CouchbaseException] =
    existsSuite + getSuite + insertSuite + removeSuite + replaceSuite + upsertSuite

  private lazy val bucketName     = BucketName("CouchbaseConnectorBucket")
  private lazy val collectionName = CollectionName("_default")
  private lazy val scopeName      = ScopeName("_default")

  private lazy val existsSuite =
    suite("exists")(
      test("succeeds if key doesn't exist") {
        val key = DocumentKey(UUID.randomUUID().toString)
        for {
          keyExists <- ZStream(QueryObject(bucketName, scopeName, collectionName, key)) >>> exists
        } yield assert(keyExists)(equalTo(false))
      },
      test("succeeds") {
        val key     = DocumentKey(UUID.randomUUID().toString)
        val content = Chunk[Byte](1, 2, 3)
        for {
          _         <- ZStream(ContentQueryObject(bucketName, scopeName, collectionName, key, content)) >>> insert
          keyExists <- ZStream(QueryObject(bucketName, scopeName, collectionName, key)) >>> exists
        } yield assertTrue(keyExists)
      }
    )

  private lazy val getSuite =
    suite("get")(
      test("fails if key doesn't exist") {
        val key   = DocumentKey(UUID.randomUUID().toString)
        val query = QueryObject(bucketName, scopeName, collectionName, key)
        for {
          keyExists <- ZStream(query) >>> exists
          wasRetrieved <- get(query).runCollect.as(true).catchSome { case _: CouchbaseException =>
                            ZIO.succeed(false)
                          }
        } yield assert(keyExists)(equalTo(false)) &&
          assert(wasRetrieved)(equalTo(false))
      },
      test("succeeds") {
        val key     = DocumentKey(UUID.randomUUID().toString)
        val content = Chunk[Byte](1, 2, 3)
        val query   = QueryObject(bucketName, scopeName, collectionName, key)
        for {
          _            <- ZStream(ContentQueryObject(bucketName, scopeName, collectionName, key, content)) >>> upsert
          wasInserted  <- ZStream(query) >>> exists
          queryContent <- get(query).runCollect
        } yield assertTrue(wasInserted) &&
          assertTrue(queryContent == content)
      }
    )

  private lazy val insertSuite =
    suite("insert")(
      test("fails if key already exists") {
        val key                = DocumentKey(UUID.randomUUID().toString)
        val content            = Chunk[Byte](1, 2, 3)
        val contentQueryObject = ContentQueryObject(bucketName, scopeName, collectionName, key, content)
        for {
          _           <- ZStream(contentQueryObject) >>> insert
          wasInserted <- ZStream(QueryObject(bucketName, scopeName, collectionName, key)) >>> exists
          wasReinserted <- (ZStream(contentQueryObject) >>> insert).as(true).catchSome { case _: CouchbaseException =>
                             ZIO.succeed(false)
                           }
        } yield assertTrue(wasInserted) &&
          assert(wasReinserted)(equalTo(false))
      },
      test("succeeds") {
        val key     = DocumentKey(UUID.randomUUID().toString)
        val content = Chunk[Byte](1, 2, 3)
        for {
          _            <- ZStream(ContentQueryObject(bucketName, scopeName, collectionName, key, content)) >>> insert
          queryContent <- get(QueryObject(bucketName, scopeName, collectionName, key)).runCollect
        } yield assertTrue(queryContent == content)
      }
    )

  private lazy val removeSuite =
    suite("remove")(
      test("fails if key doesn't exist") {
        val key   = DocumentKey(UUID.randomUUID().toString)
        val query = QueryObject(bucketName, scopeName, collectionName, key)
        for {
          keyExists <- ZStream(query) >>> exists
          wasRemoved <- (ZStream(query) >>> remove).as(true).catchSome { case _: CouchbaseException =>
                          ZIO.succeed(false)
                        }
        } yield assert(keyExists)(equalTo(false)) &&
          assert(wasRemoved)(equalTo(false))
      },
      test("succeeds") {
        val key     = DocumentKey(UUID.randomUUID().toString)
        val content = Chunk[Byte](1, 2, 3)
        val query   = QueryObject(bucketName, scopeName, collectionName, key)
        for {
          _         <- ZStream(ContentQueryObject(bucketName, scopeName, collectionName, key, content)) >>> insert
          keyExists <- ZStream(query) >>> exists
          _         <- ZStream(query) >>> remove
          wasRemoved <- get(QueryObject(bucketName, scopeName, collectionName, key)).runCollect.as(false).catchSome {
                          case _: CouchbaseException => ZIO.succeed(true)
                        }
        } yield assertTrue(keyExists) &&
          assertTrue(wasRemoved)
      }
    )

  private lazy val replaceSuite =
    suite("replace")(
      test("fails if key doesn't exist") {
        val key     = DocumentKey(UUID.randomUUID().toString)
        val content = Chunk[Byte](1, 2, 3)
        for {
          wasReplaced <- (ZStream(ContentQueryObject(bucketName, scopeName, collectionName, key, content)) >>> replace)
                           .as(true)
                           .catchSome { case _: CouchbaseException =>
                             ZIO.succeed(false)
                           }
        } yield assert(wasReplaced)(equalTo(false))
      },
      test("succeeds") {
        val key     = DocumentKey(UUID.randomUUID().toString)
        val content = Chunk[Byte](1, 2, 3)
        for {
          _            <- ZStream(ContentQueryObject(bucketName, scopeName, collectionName, key, Chunk[Byte](1))) >>> insert
          _            <- ZStream(ContentQueryObject(bucketName, scopeName, collectionName, key, content)) >>> replace
          queryContent <- get(QueryObject(bucketName, scopeName, collectionName, key)).runCollect
        } yield assertTrue(queryContent == content)
      }
    )

  private lazy val upsertSuite =
    suite("upsert")(
      test("succeeds if key doesn't exist") {
        val key     = DocumentKey(UUID.randomUUID().toString)
        val content = Chunk[Byte](1, 2, 3)
        for {
          _            <- ZStream(ContentQueryObject(bucketName, scopeName, collectionName, key, content)) >>> upsert
          queryContent <- get(QueryObject(bucketName, scopeName, collectionName, key)).runCollect
        } yield assertTrue(queryContent == content)
      },
      test("succeeds if key already exists") {
        val key      = DocumentKey(UUID.randomUUID().toString)
        val content1 = Chunk[Byte](1)
        val content2 = Chunk[Byte](1, 2, 3)
        val query    = QueryObject(bucketName, scopeName, collectionName, key)
        for {
          _             <- ZStream(ContentQueryObject(bucketName, scopeName, collectionName, key, content1)) >>> upsert
          wasInserted   <- ZStream(query) >>> exists
          firstContent  <- get(query).runCollect
          _             <- ZStream(ContentQueryObject(bucketName, scopeName, collectionName, key, content2)) >>> upsert
          secondContent <- get(query).runCollect
        } yield assertTrue(wasInserted) &&
          assertTrue(firstContent == content1) &&
          assertTrue(secondContent == content2)
      }
    )
}
