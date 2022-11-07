package zio.connect.couchbase

import com.couchbase.client.scala.Cluster
import com.couchbase.client.scala.codec.RawBinaryTranscoder
import com.couchbase.client.scala.kv.{GetOptions, InsertOptions, ReplaceOptions, UpsertOptions}
import zio.connect.couchbase.CouchbaseConnector._
import zio.stream.{ZSink, ZStream}
import zio.{Chunk, Trace, ZIO, ZLayer}

final case class LiveCouchbaseConnector(couchbase: Cluster) extends CouchbaseConnector {

  override def insert(implicit trace: Trace): ZSink[Any, CouchbaseException, ContentQueryObject, ContentQueryObject, Unit] = {
    ZSink
      .foreach[Any, Throwable, ContentQueryObject] { query =>
        ZIO.fromTry {
          couchbase
            .bucket(query.bucketName)
            .scope(query.scopeName)
            .collection(query.collectionName)
            .insert(
              query.documentKey,
              query.content.toArray,
              InsertOptions().transcoder(RawBinaryTranscoder.Instance)
            )
        }
      }
      .mapError(CouchbaseException)
  }

  override def upsert(implicit trace: Trace): ZSink[Any, CouchbaseException, ContentQueryObject, ContentQueryObject, Unit] =
    ZSink
      .foreach[Any, Throwable, ContentQueryObject] { query =>
        ZIO.fromTry {
          couchbase
            .bucket(query.bucketName)
            .scope(query.scopeName)
            .collection(query.collectionName)
            .upsert(
              query.documentKey,
              query.content.iterator.toArray,
              UpsertOptions().transcoder(RawBinaryTranscoder.Instance)
            )
        }
      }
      .mapError(CouchbaseException)

  override def get(queryObject: => QueryObject)(implicit trace: Trace): ZStream[Any, CouchbaseException, Byte] =
    ZStream
      .fromIterableZIO(
        ZIO
          .fromTry {
            couchbase
              .bucket(queryObject.bucketName)
              .scope(queryObject.scopeName)
              .collection(queryObject.collectionName)
              .get(queryObject.documentKey, GetOptions().transcoder(RawBinaryTranscoder.Instance))
              .flatMap(_.contentAs[Array[Byte]])
              .map(Chunk.fromArray)
          }
          .mapError(CouchbaseException)
      )

  override def replace(implicit trace: Trace): ZSink[Any, CouchbaseException, ContentQueryObject, ContentQueryObject, Unit] =
    ZSink
      .foreach[Any, Throwable, ContentQueryObject] { query =>
        ZIO.fromTry(
          couchbase
            .bucket(query.bucketName)
            .scope(query.scopeName)
            .collection(query.collectionName)
            .replace(
              query.documentKey,
              query.content.iterator.toArray,
              ReplaceOptions().transcoder(RawBinaryTranscoder.Instance)
            )
        )
      }
      .mapError(CouchbaseException)

  override def remove(implicit trace: Trace): ZSink[Any, CouchbaseException, QueryObject, QueryObject, Unit] = {
    ZSink
      .foreach[Any, Throwable, QueryObject] { query =>
        ZIO.fromTry(
          couchbase
            .bucket(query.bucketName)
            .scope(query.scopeName)
            .collection(query.collectionName)
            .remove(query.documentKey)
        )
      }
      .mapError(CouchbaseException)
  }

}

object LiveCouchbaseConnector {

  val layer: ZLayer[Cluster, Nothing, LiveCouchbaseConnector] =
    ZLayer.fromZIO(ZIO.service[Cluster].map(LiveCouchbaseConnector(_)))

}
