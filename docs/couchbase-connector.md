---
id: couchbase-connector
title: "Couchbase Connector"
---

Setup
-----

```scala
libraryDependencies += "dev.zio" %% "zio-connect-couchbase" % "@VERSION@"
```

How to use it?
-----

All available CouchbaseConnector combinators and operations are available in the package object `zio.connect.couchbase`, you only
need to import `zio.connect.couchbase._` to get started.

The couchbase connector presumes you already have a couchbase cluster to connect to, and uses the official java client under the hood.
You can provide a cluster connection in the usual way and wrap it in a `ZLayer`, typically something like this:

```scala
import com.couchbase.client.java.Cluster
import zio._

  val cluster = ZLayer.scoped(
    ZIO.acquireRelease(
      ZIO.attempt(
        Cluster
          .connect("127.0.0.1", "admin", "admin22")
      )
    )(c => ZIO.attempt(c.disconnect()).orDie)
  )
```

The connector provides a number of operations that can be used to interact with the cluster, most of the operations require
a `QueryObject` or `ContentQueryObject` which are case classes. The couchbase primitives that are used: `BucketName`, `CollectionName`,
`ScopeName` and `DocumentId` are all defined as zio-prelude newtypes of `String`

```scala
import zio.connect.couchbase.CouchbaseConnector._

val bucket     = BucketName("gamesim-sample")
val collection = CollectionName("_default")
val scope      = ScopeName("_default")
val newKey     = DocumentKey("zio-connect-doc")

val queryObject = QueryObject(bucket, scope, collection, newKey)
```

Now let's do something, and by do, we mean let's describe an action, like checking to see that a document exists by key/id:

```scala 
val checkExists: ZIO[CouchbaseConnector, CouchbaseException, Boolean] = ZStream(queryObject) >>> exists
```

Some important points to note here, the `exists` operation is a `ZSink` that expects elements of type `QueryObject` as its streamed input.
You can access the underlying `Throwable` from the `CouchbaseException` by using the `reason` property


Here's what inserting a document looks like:

```scala
val key                = DocumentKey(UUID.randomUUID().toString)
val content            = Chunk[Byte](1, 2, 3)
val contentQueryObject = ContentQueryObject(bucketName, scopeName, collectionName, key, content)

val insertAction: ZIO[CouchbaseConnector, CouchbaseException, Unit] = ZStream(contentQueryObject) >>> insert
```

`insert` is a `ZSink` that expects elements of type `ContentQueryObject`, which is a query object with an additional 
`content` property, as its streamed input and returns `Unit` as its output.

To get a document by key/id:

```scala
val getAction: ZIO[CouchbaseConnector, CouchbaseException, Chunk[Byte]] = get(queryObject).runCollect
```

`get` is a `ZStream` that takes a query object as an argument and returns a `ZStream` of `Chunk[Byte]` as its output. There are 
other ways to process the chunk of bytes which get returned, but of course this depends on your domain.

In order to run a program involving the couchbase connector, you need to provide the `CouchbaseConnector` layer, and the cluster connection layer we created earlier:

```scala
def run = getAction.provide(couchbaseConnectorLiveLayer, cluster)
```

`couchbaseConnectorLiveLayer` is a `ZLayer` that provides the `LiveCouchbaseConnector` service, and is defined in the `CouchbaseConnector` companion object.

Test / Stub
-----------
A stub implementation of CouchbaseConnector is provided for testing purposes via the `couchbaseConnectorTestLayer`. It uses
internally a `TRef[Map[couchbase.CouchbaseConnector.BucketName.Type, CouchbaseBucket]]` instead of talking to Couchbase. 
You can create the test harness as follows:

```scala
import zio.connect.couchbase._

object MyTestSpec extends ZIOSpecDefault {

  override def spec =
    suite("MyTestSpec")(???)
      .provide(couchbaseConnectorTestLayer)

}
```

Operators
----

The following operators are available:

## `exists` 

Checks if a document exists by key/id, if the bucket, collection or scope do not exist you will get an `AuthenticationFailureException`
complaining about privileges.

```scala
val existsAction: ZIO[CouchbaseConnector, CouchbaseException, Boolean] = ZStream(queryObject) >>> exists
```

## `get`

Gets a document by key/id, if the bucket, collection or scope do not exist you will get an `AuthenticationFailureException`.
If the document does not exist you will get a `DocumentNotFoundException`.

```scala
val getAction: ZIO[CouchbaseConnector, CouchbaseException, Chunk[Byte]] = get(queryObject).runCollect
```

## `insert`

Inserts a document by key/id, if the bucket, collection or scope do not exist you will get an `AuthenticationFailureException`.
If the document already exists you will get a `DocumentExistsException`.

```scala
val insertAction: ZIO[CouchbaseConnector, CouchbaseException, Unit] = ZStream(contentQueryObject) >>> insert
```

## `remove`

Removes a document by key/id, if the bucket, collection or scope do not exist you will get an `AuthenticationFailureException`.
If the document does not exist you will get a `DocumentNotFoundException`.

```scala
val removeAction: ZIO[CouchbaseConnector, CouchbaseException, Unit] = ZStream(queryObject) >>> remove
```

## `replace`

Replaces a document by key/id, if the bucket, collection or scope do not exist you will get an `AuthenticationFailureException`.
If the document does not exist you will get a `DocumentNotFoundException`.

```scala
val replaceAction: ZIO[CouchbaseConnector, CouchbaseException, Unit] = ZStream(contentQueryObject) >>> replace
```

## `upsert`

Updates or inserts a document by key/id, if the bucket, collection or scope do not exist you will get an `AuthenticationFailureException`.

```scala
val upsertAction: ZIO[CouchbaseConnector, CouchbaseException, Unit] = ZStream(contentQueryObject) >>> upsert
```
