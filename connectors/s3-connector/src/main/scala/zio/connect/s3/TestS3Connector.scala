package zio.connect.s3

import zio.connect.s3.S3Connector.{BucketName, CopyObject, MoveObject, ObjectKey, S3Exception}
import zio.connect.s3.TestS3Connector.S3Node.{S3Bucket, S3Obj}
import zio.connect.s3.TestS3Connector.TestS3
import zio.stm.{STM, TRef, ZSTM}
import zio.stream.{ZSink, ZStream}
import zio.{Chunk, Trace, ZIO, ZLayer}

private[s3] final case class TestS3Connector(s3: TestS3) extends S3Connector {
  override def copyObject(implicit
    trace: Trace
  ): ZSink[Any, S3Connector.S3Exception, S3Connector.CopyObject, S3Connector.CopyObject, Unit] =
    ZSink.foreach(copyObject => s3.copyObject(copyObject))

  override def createBucket(implicit trace: Trace): ZSink[Any, S3Connector.S3Exception, BucketName, BucketName, Unit] =
    ZSink.foreach(name => s3.createBucket(name))

  override def deleteEmptyBucket(implicit
    trace: Trace
  ): ZSink[Any, S3Connector.S3Exception, BucketName, BucketName, Unit] =
    ZSink.foreach(bucket => s3.deleteEmptyBucket(bucket))

  override def deleteObjects(bucketName: => BucketName)(implicit
    trace: Trace
  ): ZSink[Any, S3Connector.S3Exception, ObjectKey, ObjectKey, Unit] =
    ZSink.foreach(key => s3.deleteObject(bucketName, key))

  override def getObject(bucketName: => BucketName, key: => ObjectKey)(implicit
    trace: Trace
  ): ZStream[Any, S3Connector.S3Exception, Byte] =
    s3.getObject(bucketName, key)

  override def listBuckets(implicit trace: Trace): ZStream[Any, S3Exception, BucketName] =
    ZStream.fromIterableZIO(s3.listBuckets)

  override def listObjects(bucketName: => BucketName)(implicit
    trace: Trace
  ): ZStream[Any, S3Connector.S3Exception, ObjectKey] =
    ZStream.fromIterableZIO(s3.listObjects(bucketName))

  override def moveObject(implicit
    trace: Trace
  ): ZSink[Any, S3Connector.S3Exception, S3Connector.MoveObject, S3Connector.MoveObject, Unit] =
    ZSink.foreach(moveObject => s3.moveObject(moveObject))

  override def putObject(bucketName: => BucketName, key: => ObjectKey)(implicit
    trace: Trace
  ): ZSink[Any, S3Connector.S3Exception, Byte, Nothing, Unit] =
    ZSink.unwrap(for {
      ref <- TRef.makeCommit(false)
      r = ZSink.foreachChunk[Any, S3Connector.S3Exception, Byte] { bytes =>
            s3.putObject(bucketName, key, bytes, ref)
          }
    } yield r)
}

object TestS3Connector {

  val layer: ZLayer[Any, Nothing, TestS3Connector] =
    ZLayer.fromZIO(STM.atomically {
      for {
        a <- TRef.make(Map.empty[BucketName, S3Bucket])
      } yield TestS3Connector(TestS3(a))
    })

  private[s3] sealed trait S3Node

  private[s3] object S3Node {
    final case class S3Bucket(name: BucketName, objects: Map[ObjectKey, S3Obj]) extends S3Node

    final case class S3Obj(key: ObjectKey, content: Chunk[Byte]) extends S3Node
  }

  private[s3] final case class TestS3(repo: TRef[Map[BucketName, S3Bucket]]) {

    private def bucketDoesntExistException(name: BucketName): S3Exception =
      S3Exception(new RuntimeException(s"Bucket $name doesn't exist"))

    def copyObject(m: CopyObject): ZIO[Any, S3Exception, Unit] =
      ZSTM.atomically(for {
        map <- repo.get
        sourceBucket <-
          ZSTM
            .fromOption(map.get(m.sourceBucketName))
            .mapError(_ => bucketDoesntExistException(m.sourceBucketName))
        destinationBucket <-
          ZSTM
            .fromOption(map.get(m.targetBucketName))
            .mapError(_ => bucketDoesntExistException(m.targetBucketName))
        sourceObject <-
          ZSTM.fromOption(sourceBucket.objects.get(m.objectKey)).mapError(_ => objectDoesntExistException(m.objectKey))
        _ <- repo.update(mp =>
               mp.updated(
                 destinationBucket.name,
                 S3Bucket(destinationBucket.name, destinationBucket.objects.updated(m.objectKey, sourceObject))
               )
             )
      } yield ())

    def createBucket(name: BucketName): ZIO[Any, S3Exception, Unit] =
      ZSTM.atomically(
        for {
          bucket <- repo.get.map(_.get(name))
          _ <- if (bucket.isDefined) ZSTM.succeed(())
               else repo.getAndUpdate(m => m.updated(name, S3Bucket(name, Map.empty)))
        } yield ()
      )

    def deleteEmptyBucket(bucket: BucketName): ZIO[Any, S3Exception, Unit] =
      ZSTM.atomically(
        for {
          bucketOp <- repo.get.map(_.get(bucket))
          _ <-
            bucketOp match {
              case Some(b) =>
                if (b.objects.isEmpty) repo.getAndUpdate(m => m - bucket)
                else ZSTM.fail(S3Exception(new RuntimeException("Bucket not empty")))
              case None =>
                ZSTM.fail(S3Exception(new RuntimeException("Bucket does not exist")))
            }
        } yield ()
      )

    def deleteObject(bucket: BucketName, key: ObjectKey): ZIO[Any, S3Exception, Unit] =
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

    def getObject(bucketName: BucketName, key: ObjectKey): ZStream[Any, S3Exception, Byte] =
      ZStream.unwrap(
        ZSTM.atomically(
          for {
            map    <- repo.get
            bucket <- ZSTM.fromOption(map.get(bucketName)).orElseFail(bucketDoesntExistException(bucketName))
            obj    <- ZSTM.fromOption(bucket.objects.get(key)).orElseFail(objectDoesntExistException(key))
          } yield ZStream.fromChunk(obj.content)
        )
      )

    def listBuckets: ZIO[Any, S3Exception, Chunk[BucketName]] =
      ZSTM.atomically(
        repo.get.map(_.keys).map(Chunk.fromIterable)
      )

    def listObjects(bucketName: BucketName): ZIO[Any, S3Exception, Chunk[ObjectKey]] =
      ZSTM.atomically(
        for {
          map <- repo.get
          bucket <- ZSTM
                      .fromOption(map.get(bucketName))
                      .mapError(_ => S3Exception(new RuntimeException("No such bucket exception!")))
        } yield Chunk.fromIterable(bucket.objects.keys)
      )

    def moveObject(m: MoveObject): ZIO[Any, S3Exception, Unit] =
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
               m1.updated(sourceBucket.name, S3Bucket(sourceBucket.name, sourceBucket.objects - m.objectKey))
             }
      } yield ())

    private def objectDoesntExistException(key: ObjectKey): S3Exception =
      S3Exception(new RuntimeException(s"Object $key doesn't exist"))

    def putObject(
      bucketName: BucketName,
      key: ObjectKey,
      bytes: Chunk[Byte],
      append: TRef[Boolean]
    ): ZIO[Any, S3Connector.S3Exception, Unit] =
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
            else map.updated(bucketName, S3Bucket(bucketName, bucket.objects.updated(key, S3Obj(key, bytes))))
          _ <- repo.set(updatedMap)
          _ <- append.set(true)
        } yield ()
      )
  }

}
