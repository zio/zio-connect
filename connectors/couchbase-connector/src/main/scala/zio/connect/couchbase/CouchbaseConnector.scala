package zio.connect.couchbase

import zio.connect.couchbase.CouchbaseConnector.{ContentQueryObject, CouchbaseException, QueryObject}
import zio.prelude.Subtype
import zio.stream._
import zio.{Chunk, Trace}

trait CouchbaseConnector {

  def exists(implicit trace: Trace): ZSink[Any, CouchbaseException, QueryObject, QueryObject, Boolean]

  def get(queryObject: => QueryObject)(implicit trace: Trace): ZStream[Any, CouchbaseException, Byte]

  def insert(implicit trace: Trace): ZSink[Any, CouchbaseException, ContentQueryObject, ContentQueryObject, Unit]

  def remove(implicit trace: Trace): ZSink[Any, CouchbaseException, QueryObject, QueryObject, Unit]

  def replace(implicit trace: Trace): ZSink[Any, CouchbaseException, ContentQueryObject, ContentQueryObject, Unit]

  def upsert(implicit trace: Trace): ZSink[Any, CouchbaseException, ContentQueryObject, ContentQueryObject, Unit]

}

object CouchbaseConnector {

  object BucketName extends Subtype[String]
  type BucketName = BucketName.Type

  object CollectionName extends Subtype[String]
  type CollectionName = CollectionName.Type

  object DocumentKey extends Subtype[String]
  type DocumentKey = DocumentKey.Type

  object ScopeName extends Subtype[String]
  type ScopeName = ScopeName.Type

  final case class CouchbaseException(cause: Throwable) extends RuntimeException(cause)

  final case class ContentQueryObject(
    bucketName: BucketName,
    scopeName: ScopeName,
    collectionName: CollectionName,
    documentKey: DocumentKey,
    content: Chunk[Byte]
  )

  final case class QueryObject(
    bucketName: BucketName,
    scopeName: ScopeName,
    collectionName: CollectionName,
    documentKey: DocumentKey
  )

}
