package zio.connect.couchbase

import com.couchbase.client.scala.json.JsonObject
import zio.connect.couchbase.CouchbaseConnector.{CouchbaseException, DocumentKey, QueryObject}
import zio.prelude.Subtype
import zio.stream._


trait CouchbaseConnector {

  def insert: ZSink[Any, CouchbaseException, QueryObject, QueryObject, Unit]

  def upsert: ZSink[Any, CouchbaseException, QueryObject, QueryObject, Unit]

  def get: ZStream[Any, CouchbaseException, Byte]

  def replace: ZSink[Any, CouchbaseException, QueryObject, QueryObject, Unit]

  def remove: ZSink[Any, CouchbaseException, DocumentKey, DocumentKey, Unit]

}

object CouchbaseConnector {

  object DocumentKey extends Subtype[String]
  type DocumentKey = DocumentKey.type

  case class QueryObject(
    documentKey: DocumentKey,
    jsonObject: JsonObject
  )

  case class CouchbaseException(reason: Throwable)
}