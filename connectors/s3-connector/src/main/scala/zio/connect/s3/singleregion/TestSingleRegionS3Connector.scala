package zio.connect.s3.singleregion

import zio.aws.core.AwsError
import zio.aws.s3.model.primitives.{BucketName, ObjectKey}
import zio.connect.s3.S3Connector.{CopyObject, MoveObject}
import zio.connect.s3.singleregion.TestSingleRegionS3Connector.S3Node.{S3Bucket, S3Obj}
import zio.connect.s3.singleregion.TestSingleRegionS3Connector.TestS3
import zio.stm.{STM, TRef, ZSTM}
import zio.stream.{ZSink, ZStream}
import zio.{Chunk, Trace, ZIO, ZLayer}

private[s3] final case class TestSingleRegionS3Connector(s3: TestS3) extends SingleRegionS3Connector {

  override def copyObject(implicit
    trace: Trace
  ): ZSink[Any, AwsError, CopyObject, CopyObject, Unit] =
    ZSink.foreach(copyObject => s3.copyObject(copyObject))

  override def createBucket(implicit
    trace: Trace
  ): ZSink[Any, AwsError, BucketName, BucketName, Unit] =
    ZSink.foreach(name => s3.createBucket(name))

  override def deleteEmptyBucket(implicit
    trace: Trace
  ): ZSink[Any, AwsError, BucketName, BucketName, Unit] =
    ZSink.foreach(bucket => s3.deleteEmptyBucket(bucket))

  override def deleteObjects(bucketName: => BucketName)(implicit
    trace: Trace
  ): ZSink[Any, AwsError, ObjectKey, ObjectKey, Unit] =
    ZSink.foreach(key => s3.deleteObject(bucketName, key))

  override def getObject(bucketName: => BucketName, key: => ObjectKey)(implicit
    trace: Trace
  ): ZStream[Any, AwsError, Byte] =
    s3.getObject(bucketName, key)

  override def listBuckets(implicit trace: Trace): ZStream[Any, AwsError, BucketName] =
    ZStream.fromIterableZIO(s3.listBuckets)

  override def listObjects(bucketName: => BucketName)(implicit
    trace: Trace
  ): ZStream[Any, AwsError, ObjectKey] =
    ZStream.fromIterableZIO(s3.listObjects(bucketName))

  override def moveObject(implicit trace: Trace): ZSink[Any, AwsError, MoveObject, MoveObject, Unit] =
    ZSink.foreach(m => s3.moveObject(m))

  override def putObject(bucketName: => BucketName, key: => ObjectKey)(implicit
    trace: Trace
  ): ZSink[Any, AwsError, Byte, Nothing, Unit] =
    ZSink.unwrap(for {
      ref <- TRef.makeCommit(false)
      r = ZSink.foreachChunk[Any, AwsError, Byte] { bytes =>
            s3.putObject(bucketName, key, bytes, ref)
          }
    } yield r)
}

object TestSingleRegionS3Connector {

  val layer: ZLayer[Any, Nothing, TestSingleRegionS3Connector] =
    ZLayer.fromZIO(STM.atomically {
      for {
        a <- TRef.make(Map.empty[BucketName, S3Bucket])
      } yield TestSingleRegionS3Connector(TestS3(a))
    })

  private[s3] sealed trait S3Node

  private[s3] object S3Node {
    final case class S3Bucket(name: BucketName, objects: Map[ObjectKey, S3Obj]) extends S3Node

    final case class S3Obj(key: ObjectKey, content: Chunk[Byte]) extends S3Node
  }

  private[singleregion] final case class TestS3(repo: TRef[Map[BucketName, S3Bucket]]) {
    private def bucketDoesntExistException(name: BucketName): AwsError =
      AwsError.fromThrowable(new RuntimeException(s"Bucket $name doesn't exist"))

    private def getBucket(map: Map[BucketName, S3Bucket], bucketName: BucketName) =
      ZSTM
        .fromOption(map.get(bucketName))
        .mapError(_ => bucketDoesntExistException(bucketName))

    def copyObject(m: CopyObject): ZIO[Any, AwsError, Unit] =
      ZSTM.atomically(for {
        map               <- repo.get
        sourceBucket      <- getBucket(map, m.sourceBucketName)
        destinationBucket <- getBucket(map, m.targetBucketName)
        sourceObject <-
          ZSTM.fromOption(sourceBucket.objects.get(m.objectKey)).mapError(_ => objectDoesntExistException(m.objectKey))
        _ <- repo.update(mp =>
               mp.updated(
                 (destinationBucket.name),
                 S3Bucket(destinationBucket.name, destinationBucket.objects.updated(m.objectKey, sourceObject))
               )
             )
      } yield ())

    def createBucket(name: BucketName): ZIO[Any, AwsError, Unit] =
      ZSTM.atomically(
        for {
          bucketAlreadyExists <- repo.get.map(m => m.keys.toList.contains(name))
          _ <- if (bucketAlreadyExists) ZSTM.fail(AwsError.fromThrowable(new RuntimeException("Bucket already exists")))
               else repo.getAndUpdate(m => m.updated(name, S3Bucket(name, Map.empty)))
        } yield ()
      )

    def deleteEmptyBucket(bucket: BucketName): ZIO[Any, AwsError, Unit] =
      ZSTM.atomically(
        for {
          bucketOp <- repo.get.map(_.get(bucket))
          _ <-
            bucketOp match {
              case Some(b) =>
                if (b.objects.isEmpty) repo.getAndUpdate(m => m - bucket)
                else ZSTM.fail(AwsError.fromThrowable(new RuntimeException("Bucket not empty")))
              case None =>
                ZSTM.fail(AwsError.fromThrowable(new RuntimeException("Bucket does not exist")))
            }
        } yield ()
      )

    def deleteObject(bucket: BucketName, key: ObjectKey): ZIO[Any, AwsError, Unit] =
      ZSTM.atomically(
        for {
          map <- repo.get
          bucket <-
            ZSTM
              .fromOption(map.get(bucket))
              .mapError(_ => bucketDoesntExistException(bucket))
          _ <- repo.set(map.updated(bucket.name, S3Bucket(bucket.name, bucket.objects - key)))
        } yield ()
      )

    def getObject(bucketName: BucketName, key: ObjectKey): ZStream[Any, AwsError, Byte] =
      ZStream.unwrap(
        ZSTM.atomically(
          for {
            map <- repo.get
            bucket <-
              ZSTM.fromOption(map.get(bucketName)).orElseFail(bucketDoesntExistException(bucketName))
            obj <- ZSTM.fromOption(bucket.objects.get(key)).orElseFail(objectDoesntExistException(key))
          } yield ZStream.fromChunk(obj.content)
        )
      )

    def listBuckets: ZIO[Any, AwsError, Chunk[BucketName]] =
      ZSTM.atomically(
        repo.get.map(_.keys).map(k => Chunk.fromIterable(k))
      )

    def listObjects(bucketName: BucketName): ZIO[Any, AwsError, Chunk[ObjectKey]] =
      ZSTM.atomically(
        for {
          map <- repo.get
          bucket <- ZSTM
                      .fromOption(map.get(bucketName))
                      .mapError { _ =>
                        AwsError.fromThrowable(new RuntimeException("No such bucket!"))
                      }
        } yield Chunk.fromIterable(bucket.objects.keys)
      )

    def moveObject(m: MoveObject): ZIO[Any, AwsError, Unit] =
      ZSTM.atomically(for {
        map <- repo.get
        sourceBucket <-
          ZSTM
            .fromOption(map.get(m.bucketName))
            .mapError(_ => bucketDoesntExistException(m.bucketName))
        destinationBucket <-
          ZSTM
            .fromOption(map.get(m.targetBucketName))
            .mapError(_ => bucketDoesntExistException(m.targetBucketName))
        sourceObject <-
          ZSTM.fromOption(sourceBucket.objects.get(m.objectKey)).mapError(_ => objectDoesntExistException(m.objectKey))
        _ <- repo.update { mp =>
               val m1 = mp.updated(
                 destinationBucket.name,
                 S3Bucket(destinationBucket.name, destinationBucket.objects.updated(m.targetObjectKey, sourceObject))
               )
               m1.updated(
                 sourceBucket.name,
                 S3Bucket(sourceBucket.name, sourceBucket.objects - m.objectKey)
               )
             }
      } yield ())

    private def objectDoesntExistException(key: ObjectKey): AwsError =
      AwsError.fromThrowable(new RuntimeException(s"Object $key doesn't exist"))

    def putObject(
      bucketName: BucketName,
      key: ObjectKey,
      bytes: Chunk[Byte],
      append: TRef[Boolean]
    ): ZIO[Any, AwsError, Unit] =
      ZSTM.atomically(
        for {
          map       <- repo.get
          appending <- append.get
          bucket <- ZSTM
                      .fromOption(map.get(bucketName))
                      .mapError(_ => bucketDoesntExistException(bucketName))
          updatedMap =
            if (appending)
              map.updated(
                bucketName,
                S3Bucket(
                  bucketName,
                  bucket.objects.updated(
                    key,
                    bucket.objects.get(key).map(o => S3Obj(key, o.content ++ bytes)).getOrElse(S3Obj(key, bytes))
                  )
                )
              )
            else
              map.updated(
                bucketName,
                S3Bucket(bucketName, bucket.objects.updated(key, S3Obj(key, bytes)))
              )
          _ <- repo.set(updatedMap)
          _ <- append.set(true)
        } yield ()
      )
  }

}
