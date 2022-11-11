package zio.connect.couchbase

import zio.{Chunk, Trace, ZIO, ZLayer}
import zio.connect.couchbase.CouchbaseConnector.{
  BucketName,
  CollectionName,
  ContentQueryObject,
  CouchbaseException,
  DocumentKey,
  QueryObject,
  ScopeName
}
import zio.connect.couchbase.TestCouchbaseConnector.CouchbaseNode.{
  CouchbaseBucket,
  CouchbaseCollection,
  CouchbaseDocument,
  CouchbaseScope
}
import zio.connect.couchbase.TestCouchbaseConnector.TestCouchbase
import zio.stm.{STM, TRef, ZSTM}
import zio.stream.{ZSink, ZStream}

private[couchbase] final case class TestCouchbaseConnector(couchbase: TestCouchbase) extends CouchbaseConnector {
  override def exists(implicit trace: Trace): ZSink[Any, CouchbaseException, QueryObject, QueryObject, Boolean] =
    ZSink
      .take[QueryObject](1)
      .map(_.headOption)
      .mapZIO {
        case Some(query) => couchbase.exists(query)
        case None        => ZIO.succeed(false)
      }

  override def get(queryObject: => QueryObject)(implicit trace: Trace): ZStream[Any, CouchbaseException, Byte] =
    couchbase.get(queryObject)

  override def insert(implicit
    trace: Trace
  ): ZSink[Any, CouchbaseException, ContentQueryObject, ContentQueryObject, Unit] =
    ZSink.foreach(couchbase.insert)

  override def remove(implicit trace: Trace): ZSink[Any, CouchbaseException, QueryObject, QueryObject, Unit] =
    ZSink.foreach(couchbase.remove)

  override def replace(implicit
    trace: Trace
  ): ZSink[Any, CouchbaseException, ContentQueryObject, ContentQueryObject, Unit] =
    ZSink.foreach(couchbase.replace)

  override def upsert(implicit
    trace: Trace
  ): ZSink[Any, CouchbaseException, ContentQueryObject, ContentQueryObject, Unit] =
    ZSink.foreach(couchbase.upsert)
}

object TestCouchbaseConnector {

  val layer: ZLayer[Any, Nothing, TestCouchbaseConnector] =
    ZLayer.fromZIO(STM.atomically {
      val bucket     = BucketName("CouchbaseConnectorBucket")
      val scope      = ScopeName("_default")
      val collection = CollectionName("_default")
      for {
        cluster <-
          TRef.make(
            Map(
              bucket -> CouchbaseBucket(
                bucket,
                Map(
                  scope -> CouchbaseScope(
                    scope,
                    Map(collection -> CouchbaseCollection(collection, Map.empty[DocumentKey, CouchbaseDocument]))
                  )
                )
              )
            )
          )
      } yield TestCouchbaseConnector(TestCouchbase(cluster))
    })

  private[couchbase] sealed trait CouchbaseNode

  private[couchbase] object CouchbaseNode {
    final case class CouchbaseBucket(name: BucketName, scopes: Map[ScopeName, CouchbaseScope]) extends CouchbaseNode

    final case class CouchbaseCollection(name: CollectionName, documents: Map[DocumentKey, CouchbaseDocument])
        extends CouchbaseNode

    final case class CouchbaseDocument(name: DocumentKey, content: Chunk[Byte]) extends CouchbaseNode

    final case class CouchbaseScope(name: ScopeName, collections: Map[CollectionName, CouchbaseCollection])
        extends CouchbaseNode
  }

  private[couchbase] final case class TestCouchbase(cluster: TRef[Map[BucketName, CouchbaseBucket]]) {

    private def bucketDoesNotExistException(name: BucketName): CouchbaseException =
      CouchbaseException(new RuntimeException(s"Bucket $name does not exist"))

    private def collectionDoesNotExistException(name: CollectionName): CouchbaseException =
      CouchbaseException(new RuntimeException(s"Collection $name does not exist"))

    private def documentAlreadyExistsException(key: DocumentKey): CouchbaseException =
      CouchbaseException(new RuntimeException(s"Document $key already exists"))

    private def documentDoesNotExistException(key: DocumentKey): CouchbaseException =
      CouchbaseException(new RuntimeException(s"Document $key does not exist"))

    private def scopeDoesNotExistException(name: ScopeName): CouchbaseException =
      CouchbaseException(new RuntimeException(s"Scope $name does not exist"))

    def exists(query: QueryObject): ZIO[Any, CouchbaseException, Boolean] =
      ZSTM.atomically(
        for {
          database <- cluster.get
          bucket <-
            ZSTM.fromOption(database.get(query.bucketName)).orElseFail(bucketDoesNotExistException(query.bucketName))
          scope <-
            ZSTM.fromOption(bucket.scopes.get(query.scopeName)).orElseFail(scopeDoesNotExistException(query.scopeName))
          collection <- ZSTM
                          .fromOption(scope.collections.get(query.collectionName))
                          .orElseFail(collectionDoesNotExistException(query.collectionName))
          exists <- ZSTM.succeedNow(collection.documents.exists(_._1 == query.documentKey))
        } yield exists
      )

    def get(query: QueryObject): ZStream[Any, CouchbaseException, Byte] =
      ZStream.unwrap(
        ZSTM.atomically(
          for {
            database <- cluster.get
            bucket <-
              ZSTM.fromOption(database.get(query.bucketName)).orElseFail(bucketDoesNotExistException(query.bucketName))
            scope <- ZSTM
                       .fromOption(bucket.scopes.get(query.scopeName))
                       .orElseFail(scopeDoesNotExistException(query.scopeName))
            collection <- ZSTM
                            .fromOption(scope.collections.get(query.collectionName))
                            .orElseFail(collectionDoesNotExistException(query.collectionName))
            document <- ZSTM
                          .fromOption(collection.documents.get(query.documentKey))
                          .orElseFail(documentDoesNotExistException(query.documentKey))
          } yield ZStream.fromChunk(document.content)
        )
      )

    def insert(query: ContentQueryObject): ZIO[Any, CouchbaseException, Unit] =
      ZSTM.atomically(
        for {
          database <- cluster.get
          bucket <-
            ZSTM.fromOption(database.get(query.bucketName)).orElseFail(bucketDoesNotExistException(query.bucketName))
          scope <-
            ZSTM.fromOption(bucket.scopes.get(query.scopeName)).orElseFail(scopeDoesNotExistException(query.scopeName))
          collection <- ZSTM
                          .fromOption(scope.collections.get(query.collectionName))
                          .orElseFail(collectionDoesNotExistException(query.collectionName))
          _ <- if (collection.documents.contains(query.documentKey))
                 ZSTM.fail(documentAlreadyExistsException(query.documentKey))
               else
                 cluster.getAndUpdate(db =>
                   db.updated(
                     bucket.name,
                     CouchbaseBucket(
                       bucket.name,
                       bucket.scopes.updated(
                         scope.name,
                         CouchbaseScope(
                           scope.name,
                           scope.collections.updated(
                             collection.name,
                             CouchbaseCollection(
                               collection.name,
                               collection.documents
                                 .updated(query.documentKey, CouchbaseDocument(query.documentKey, query.content))
                             )
                           )
                         )
                       )
                     )
                   )
                 )
        } yield ()
      )

    def remove(query: QueryObject): ZIO[Any, CouchbaseException, Unit] =
      ZSTM.atomically(
        for {
          database <- cluster.get
          bucket <-
            ZSTM.fromOption(database.get(query.bucketName)).orElseFail(bucketDoesNotExistException(query.bucketName))
          scope <-
            ZSTM.fromOption(bucket.scopes.get(query.scopeName)).orElseFail(scopeDoesNotExistException(query.scopeName))
          collection <- ZSTM
                          .fromOption(scope.collections.get(query.collectionName))
                          .orElseFail(collectionDoesNotExistException(query.collectionName))
          _ <-
            if (!collection.documents.contains(query.documentKey))
              ZSTM.fail(documentDoesNotExistException(query.documentKey))
            else
              cluster.getAndUpdate(db =>
                db.updated(
                  bucket.name,
                  CouchbaseBucket(
                    bucket.name,
                    bucket.scopes.updated(
                      scope.name,
                      CouchbaseScope(
                        scope.name,
                        scope.collections.updated(
                          collection.name,
                          CouchbaseCollection(collection.name, collection.documents - query.documentKey)
                        )
                      )
                    )
                  )
                )
              )
        } yield ()
      )

    def replace(query: ContentQueryObject): ZIO[Any, CouchbaseException, Unit] =
      ZSTM.atomically(
        for {
          database <- cluster.get
          bucket <-
            ZSTM.fromOption(database.get(query.bucketName)).orElseFail(bucketDoesNotExistException(query.bucketName))
          scope <-
            ZSTM.fromOption(bucket.scopes.get(query.scopeName)).orElseFail(scopeDoesNotExistException(query.scopeName))
          collection <- ZSTM
                          .fromOption(scope.collections.get(query.collectionName))
                          .orElseFail(collectionDoesNotExistException(query.collectionName))
          _ <-
            if (!collection.documents.contains(query.documentKey))
              ZSTM.fail(documentDoesNotExistException(query.documentKey))
            else
              cluster.getAndUpdate(db =>
                db.updated(
                  bucket.name,
                  CouchbaseBucket(
                    bucket.name,
                    bucket.scopes.updated(
                      scope.name,
                      CouchbaseScope(
                        scope.name,
                        scope.collections.updated(
                          collection.name,
                          CouchbaseCollection(
                            collection.name,
                            collection.documents
                              .updated(query.documentKey, CouchbaseDocument(query.documentKey, query.content))
                          )
                        )
                      )
                    )
                  )
                )
              )
        } yield ()
      )

    def upsert(query: ContentQueryObject): ZIO[Any, CouchbaseException, Unit] =
      ZSTM.atomically(
        for {
          database <- cluster.get
          bucket <-
            ZSTM.fromOption(database.get(query.bucketName)).orElseFail(bucketDoesNotExistException(query.bucketName))
          scope <-
            ZSTM.fromOption(bucket.scopes.get(query.scopeName)).orElseFail(scopeDoesNotExistException(query.scopeName))
          collection <- ZSTM
                          .fromOption(scope.collections.get(query.collectionName))
                          .orElseFail(collectionDoesNotExistException(query.collectionName))
          _ <- cluster.getAndUpdate(db =>
                 db.updated(
                   bucket.name,
                   CouchbaseBucket(
                     bucket.name,
                     bucket.scopes.updated(
                       scope.name,
                       CouchbaseScope(
                         scope.name,
                         scope.collections.updated(
                           collection.name,
                           CouchbaseCollection(
                             collection.name,
                             collection.documents
                               .updated(query.documentKey, CouchbaseDocument(query.documentKey, query.content))
                           )
                         )
                       )
                     )
                   )
                 )
               )
        } yield ()
      )
  }

}
