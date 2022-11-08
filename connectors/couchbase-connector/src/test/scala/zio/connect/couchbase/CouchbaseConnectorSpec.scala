package zio.connect.couchbase

import zio.connect.couchbase.CouchbaseConnector.{BucketName, CollectionName, ContentQueryObject, CouchbaseException, DocumentKey, QueryObject, ScopeName}
import zio.stream.ZStream
import zio.test.Assertion.equalTo
import zio.test._
import zio.{Chunk, ZIO}

import java.util.UUID

trait CouchbaseConnectorSpec extends ZIOSpecDefault {

  val couchbaseConnectorSpec =
    insertSuite + upsertSuite + getSuite + replaceSuite + removeSuite

  private lazy val bucketName = BucketName("CouchbaseConnectorBucket")
  private lazy val scopeName = ScopeName("_default")
  private lazy val collectionName = CollectionName("_default")

  private lazy val insertSuite =
    suite("insert")(
      test("fails if key already exists") {
        val key                = DocumentKey(UUID.randomUUID().toString)
        val content            = Chunk[Byte](1, 2, 3)
        val contentQueryObject = ContentQueryObject(bucketName, scopeName, collectionName, key, content)
        for {
          _           <- ZStream.succeed(contentQueryObject) >>> insert
          wasInserted <- (ZStream.succeed(contentQueryObject) >>> insert).as(true)
            .catchSome {
              case _: CouchbaseException => ZIO.succeed(false)
            }
        } yield assert(wasInserted)(equalTo(false))
      },
      test("succeeds") {
        val key     = DocumentKey(UUID.randomUUID().toString)
        val content = Chunk[Byte](1, 2, 3)
        for {
          _            <- ZStream.succeed(ContentQueryObject(bucketName, scopeName, collectionName, key, content)) >>> insert
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
          _            <- ZStream.succeed(ContentQueryObject(bucketName, scopeName, collectionName, key, content)) >>> upsert
          queryContent <- get(QueryObject(bucketName, scopeName, collectionName, key)).runCollect
        } yield assertTrue(queryContent == content)
      },
      test("succeeds if key already exists") {
        val key     = DocumentKey(UUID.randomUUID().toString)
        val content = Chunk[Byte](1, 2, 3)
        for {
          _            <- ZStream.succeed(ContentQueryObject(bucketName, scopeName, collectionName, key, Chunk[Byte](1))) >>> upsert
          _            <- ZStream.succeed(ContentQueryObject(bucketName, scopeName, collectionName, key, content)) >>> upsert
          queryContent <- get(QueryObject(bucketName, scopeName, collectionName, key)).runCollect
        } yield assertTrue(queryContent == content)
      },
    )

  private lazy val getSuite =
    suite("get")(
      test("fails if key doesn't exist") {
        val key = DocumentKey(UUID.randomUUID().toString)
        for {
          wasRetrieved <- get(QueryObject(bucketName, scopeName, collectionName, key)).runCollect.as(true)
            .catchSome {
              case _: CouchbaseException => ZIO.succeed(false)
            }
        } yield assert(wasRetrieved)(equalTo(false))
      },
      test("succeeds") {
        val key     = DocumentKey(UUID.randomUUID().toString)
        val content = Chunk[Byte](1, 2, 3)
        for {
          _            <- ZStream.succeed(ContentQueryObject(bucketName, scopeName, collectionName, key, content)) >>> upsert
          queryContent <- get(QueryObject(bucketName, scopeName, collectionName, key)).runCollect
        } yield assertTrue(queryContent == content)
      }
    )

  private lazy val replaceSuite =
    suite("replace")(
      test("fails if key doesn't exist") {
        val key     = DocumentKey(UUID.randomUUID().toString)
        val content = Chunk[Byte](1, 2, 3)
        for {
          wasReplaced <- (ZStream.succeed(ContentQueryObject(bucketName, scopeName, collectionName, key, content)) >>> replace).as(true)
            .catchSome {
              case _: CouchbaseException => ZIO.succeed(false)
            }
        } yield assert(wasReplaced)(equalTo(false))
      },
      test("succeeds") {
        val key     = DocumentKey(UUID.randomUUID().toString)
        val content = Chunk[Byte](1, 2, 3)
        for {
          _            <- ZStream.succeed(ContentQueryObject(bucketName, scopeName, collectionName, key, Chunk[Byte](1))) >>> insert
          _            <- ZStream.succeed(ContentQueryObject(bucketName, scopeName, collectionName, key, content)) >>> replace
          queryContent <- get(QueryObject(bucketName, scopeName, collectionName, key)).runCollect
        } yield assertTrue(queryContent == content)
      }
    )

  private lazy val removeSuite =
    suite("remove")(
      test("fails if key doesn't exist") {
        val key = DocumentKey(UUID.randomUUID().toString)
        for {
          wasRemoved <- (ZStream.succeed(QueryObject(bucketName, scopeName, collectionName, key)) >>> remove).as(true)
            .catchSome {
              case _: CouchbaseException => ZIO.succeed(false)
            }
        } yield assert(wasRemoved)(equalTo(false))
      },
      test("succeeds") {
        val key     = DocumentKey(UUID.randomUUID().toString)
        val content = Chunk[Byte](1, 2, 3)
        for {
          _          <- ZStream.succeed(ContentQueryObject(bucketName, scopeName, collectionName, key, content)) >>> insert
          _          <- ZStream.succeed(QueryObject(bucketName, scopeName, collectionName, key)) >>> remove
          wasRemoved <- get(QueryObject(bucketName, scopeName, collectionName, key)).runCollect.as(false)
            .catchSome {
              case _: CouchbaseException => ZIO.succeed(true)
            }
        } yield assertTrue(wasRemoved)
      }
    )
}
