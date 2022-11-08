package zio.connect.couchbase

import zio.connect.couchbase.CouchbaseConnector.{ContentQueryObject, CouchbaseException, QueryObject}
import zio.prelude.Subtype
import zio.stream._
import zio.{Chunk, Trace}

trait CouchbaseConnector {

  def insert(implicit trace: Trace): ZSink[Any, CouchbaseException, ContentQueryObject, ContentQueryObject, Unit]

  def upsert(implicit trace: Trace): ZSink[Any, CouchbaseException, ContentQueryObject, ContentQueryObject, Unit]

  def get(queryObject: => QueryObject)(implicit trace: Trace): ZStream[Any, CouchbaseException, Byte]

  def replace(implicit trace: Trace): ZSink[Any, CouchbaseException, ContentQueryObject, ContentQueryObject, Unit]

  def remove(implicit trace: Trace): ZSink[Any, CouchbaseException, QueryObject, QueryObject, Unit]

}

object CouchbaseConnector {

  object BucketName extends Subtype[String]
  type BucketName = BucketName.Type

  object ScopeName extends Subtype[String]
  type ScopeName = ScopeName.Type

  object CollectionName extends Subtype[String]
  type CollectionName = CollectionName.Type

  object DocumentKey extends Subtype[String]
  type DocumentKey = DocumentKey.Type

  case class QueryObject(
                          bucketName: BucketName,
                          scopeName: ScopeName,
                          collectionName: CollectionName,
                          documentKey: DocumentKey
                        )

  case class ContentQueryObject(
                                 bucketName: BucketName,
                                 scopeName: ScopeName,
                                 collectionName: CollectionName,
                                 documentKey: DocumentKey,
                                 content: Chunk[Byte]
                               )

  case class CouchbaseException(reason: Throwable)

}
