---
id: s3-connector
title: "S3 Connector"
---

Setup
-----

```scala
libraryDependencies += "dev.zio" %% "zio-connect-s3" % "@VERSION@"
```

How to use it?
-----

All available S3Connector combinators and operations are available in the package object `zio.connect.s3`, you only need to import `zio.connect.s3._`

First, you must configure the underlying S3 connection provided by `zio-aws` you can read more about how to configure it [here][zio-aws]
If you have default credentials in the system environment typically at `~/.aws/credentials` or as env variables
the following configuration will likely work.

[zio-aws]: https://zio.github.io/zio-aws/docs/overview/overview_config

```scala
import zio._
import zio.connect.s3._
import zio.stream._
import zio.aws.core.config.AwsConfig
import zio.aws.netty.NettyHttpClient

lazy val zioAwsConfig = NettyHttpClient.default >>> AwsConfig.default
```

Now let's create a bucket:

```scala
val bucketName = BucketName("this-very-charming-bucket-name") // BucketName is a zio prelude newtype of String

val program1: ZIO[S3Connector, S3Exception, Unit] =
  for {
    _ <- ZStream(bucketName) >>> createBucket
  } yield ()
```

The way to understand this is to recognize that `createBucket` is a `ZSink` that expects elements of type `BucketName` as its streamed input.
In this case we have a `ZStream` with a single element of type `BucketName` but we could have an arbitrary number of buckets and the code
would look and work virtually the same.

Okay, let's put some readable bytes into that bucket:

```scala
val objectKey = ObjectKey("my-object") // ObjectKey is a zio prelude newtype of String

val program2: ZIO[S3Connector, S3Exception, Unit] =
  for {
    content <- Random.nextString(100).map(_.getBytes).map(Chunk.fromArray)
    _       <- ZStream.fromChunk(content) >>> putObject(bucketName, objectKey)
  } yield ()
```

Here a stream of chunks of bytes are streamed into the `putObject` sink. The sink takes two arguments, the bucket name and the object key to associate with the data
being streamed in.

Let's list objects in the bucket:

```scala
val program3: ZIO[S3Connector, S3Exception, Chunk[ObjectKey]] =
  for {
    keys <- listObjects(bucketName).runCollect
  } yield keys
```

`listObjects` is a `ZStream` that emits elements of type `ObjectKey` and we can use the `runCollect` operator to collect 
all the elements into a `Chunk`.

Here's what it looks like to get an object put earlier:

```scala
val program5: ZIO[S3Connector, Object, String] =
  for {
    content <- getObject(bucketName, objectKey) >>> ZPipeline.utf8Decode >>> ZSink.mkString
  } yield content
```

Finally, let's look at how to run one of these programs:

```scala
def run = program1.provide(zioAwsConfig, S3.live, s3ConnectorLiveLayer)
```

You need to provide the configuration layer for `zio-aws`, the `S3` layer from `zio-aws` and the `s3ConnectorLiveLayer` 
which is the live implementation of the `S3Connector` interface.

Test / Stub
-----------
A stub implementation of S3Connector is provided for testing purposes via the `TestS3Connector.layer`. It uses
internally an `TRef[Map[BucketName, S3Bucket]]` instead of talking to S3. You can create the test harness as follows:

```scala
import zio.connect.s3._

object MyTestSpec extends ZIOSpecDefault {

  override def spec =
    suite("MyTestSpec")(???)
      .provide(s3ConnectorTestLayer)

}
```

Operators & Examples
----

The following operators are available:

## `copyObject` 

Copy an object from one bucket to another

```scala
ZStream(CopyObject(bucket1, objectKey, bucket2)) >>> copyObject
```

## `createBucket`

Creates S3 buckets

```scala
ZStream(bucketName1, bucketName2) >>> createBucket
```

## `deleteEmptyBucket` 

Deletes empty S3 buckets 

```scala
ZStream(bucketName1, bucketName2) >>> deleteEmptyBucket
```
The buckets must be empty, if they are not you will get an `BucketsNotEmptyException` from S3


## `deleteObjects` 

Deletes objects from an S3 bucket

```scala
ZStream(objectKey1, objectKey2) >>> deleteObjects(bucketName)
```
Does not result in an error, if object keys do not exist


## `existsBucket` 

Checks if a bucket exists

```scala
ZStream(bucketName1, bucketName2) >>> existsBucket
```

## `existsObject`

Checks if an object exists in an s3 bucket

```scala
ZStream(objectKey1, objectKey2) >>> existsObject(bucketName)
```
It expects the bucket to exist and will return a `NoSuchBucketException` if the _bucket_ does not


## `getObject`

Gets an object from an S3 bucket

```scala
getObject(bucket2, objectKey) >>> ZPipeline.utf8Decode >>> ZSink.mkString
```
You will receive the objects as a stream of bytes, parsing/decoding of course depends on the object contents.
The example here assumes you have a stream of utf-8 encoded bytes and you want to decode them into a string.


## `listBuckets`

Lists all buckets in the account

```scala
listBuckets >>> ZSink.collectAll
```
Currently, gets ALL buckets, there is no pagination support yet. You may want to use some other ZStream combinators
to filter the lists prior to collecting bucket names


## `listObjects`

Lists all objects keys in a bucket takes a `BucketName` as an argument

```scala
listObjects(bucketName) >>> ZSink.collectAll
```
Currently, gets ALL objects in the bucket, there is no pagination support yet. You may want to use some other ZStream combinators
to filter the lists prior to collecting object keys


## `moveObject`

Move an object from one bucket to another

```scala
ZStream(MoveObject(sourceBucket, sourceKey, targetBucket, targetKey)) >>> moveObject
```
The `sourceBucket`, `sourceKey`, and `targetBucket` must exist. If the `targetKey` exists, it will be overwritten.


## `putObject`

Puts an object into an S3 bucket

```scala
ZStream.fromChunk(content) >>> putObject(bucketName, objectKey)
```
Expects as stream of bytes, returns a `Unit` if successful.
