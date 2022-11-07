package zio.connect

import com.couchbase.client.scala.Cluster
import zio.connect.couchbase.CouchbaseConnector.{ContentQueryObject, CouchbaseException, QueryObject}
import zio.stream.{ZSink, ZStream}
import zio.{Trace, ZLayer}

package object couchbase {

  def insert(implicit trace: Trace): ZSink[CouchbaseConnector, CouchbaseException, ContentQueryObject, ContentQueryObject, Unit] =
    ZSink.serviceWithSink(_.insert)

  def upsert(implicit trace: Trace): ZSink[CouchbaseConnector, CouchbaseException, ContentQueryObject, ContentQueryObject, Unit] =
    ZSink.serviceWithSink(_.upsert)

  def get(queryObject: => QueryObject)(implicit trace: Trace): ZStream[CouchbaseConnector, CouchbaseException, Byte] =
    ZStream.serviceWithStream[CouchbaseConnector](_.get(queryObject))

  def replace(implicit trace: Trace): ZSink[CouchbaseConnector, CouchbaseException, ContentQueryObject, ContentQueryObject, Unit] =
    ZSink.serviceWithSink(_.replace)

  def remove(implicit trace: Trace): ZSink[CouchbaseConnector, CouchbaseException, QueryObject, QueryObject, Unit] =
    ZSink.serviceWithSink(_.remove)

  val couchbaseConnectorLiveLayer: ZLayer[Cluster, Nothing, LiveCouchbaseConnector] = LiveCouchbaseConnector.layer

}
