package zio.connect

import com.couchbase.client.scala.Cluster
import zio.connect.couchbase.CouchbaseConnector.{ContentQueryObject, CouchbaseException, QueryObject}
import zio.stream.{ZSink, ZStream}
import zio.{Trace, ZLayer}

package object couchbase {

  def exists(implicit trace: Trace): ZSink[CouchbaseConnector, CouchbaseException, QueryObject, QueryObject, Boolean] =
    ZSink.serviceWithSink(_.exists)

  def get(queryObject: => QueryObject)(implicit trace: Trace): ZStream[CouchbaseConnector, CouchbaseException, Byte] =
    ZStream.serviceWithStream[CouchbaseConnector](_.get(queryObject))

  def insert(implicit
    trace: Trace
  ): ZSink[CouchbaseConnector, CouchbaseException, ContentQueryObject, ContentQueryObject, Unit] =
    ZSink.serviceWithSink(_.insert)

  def remove(implicit trace: Trace): ZSink[CouchbaseConnector, CouchbaseException, QueryObject, QueryObject, Unit] =
    ZSink.serviceWithSink(_.remove)

  def replace(implicit
    trace: Trace
  ): ZSink[CouchbaseConnector, CouchbaseException, ContentQueryObject, ContentQueryObject, Unit] =
    ZSink.serviceWithSink(_.replace)

  def upsert(implicit
    trace: Trace
  ): ZSink[CouchbaseConnector, CouchbaseException, ContentQueryObject, ContentQueryObject, Unit] =
    ZSink.serviceWithSink(_.upsert)

  val couchbaseConnectorLiveLayer: ZLayer[Cluster, Nothing, LiveCouchbaseConnector] = LiveCouchbaseConnector.layer
  val couchbaseConnectorTestLayer: ZLayer[Any, Nothing, TestCouchbaseConnector]     = TestCouchbaseConnector.layer

}
