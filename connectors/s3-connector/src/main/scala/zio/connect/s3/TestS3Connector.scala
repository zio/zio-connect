package zio.connect.s3

import software.amazon.awssdk.services.s3.model.NoSuchBucketException
import zio.connect.s3.S3Connector.{CopyObject, MoveObject, S3Exception}
import zio.connect.s3.TestS3Connector.S3Node.{S3Bucket, S3Obj}
import zio.connect.s3.TestS3Connector.TestS3
import zio.stm.{STM, TRef, ZSTM}
import zio.stream.{ZSink, ZStream}
import zio.{Chunk, Trace, ZIO, ZLayer}

case class TestS3Connector(s3: TestS3) extends S3Connector {
  override def copyObject(implicit
    trace: Trace
  ): ZSink[Any, S3Connector.S3Exception, S3Connector.CopyObject, S3Connector.CopyObject, Unit] =
    ZSink.foreach(copyObject => s3.copyObject(copyObject))

  override def createBucket(implicit trace: Trace): ZSink[Any, S3Connector.S3Exception, String, String, Unit] =
    ZSink.foreach(name => s3.createBucket(name))

  override def deleteEmptyBucket(implicit trace: Trace): ZSink[Any, S3Connector.S3Exception, String, String, Unit] =
    ZSink.foreach(bucket => s3.deleteEmptyBucket(bucket))

  override def deleteObjects(bucketName: => String)(implicit
    trace: Trace
  ): ZSink[Any, S3Connector.S3Exception, String, String, Unit] =
    ZSink.foreach(key => s3.deleteObject(bucketName, key))

  override def getObject(bucketName: => String, key: String)(implicit
    trace: Trace
  ): ZStream[Any, S3Connector.S3Exception, Byte] =
    s3.getObject(bucketName, key)

  override def listBuckets(implicit trace: Trace): ZStream[Any, S3Exception, String] =
    ZStream.fromIterableZIO(s3.listBuckets)

  override def listObjects(bucketName: => String)(implicit
    trace: Trace
  ): ZStream[Any, S3Connector.S3Exception, String] =
    ZStream.fromIterableZIO(s3.listObjects(bucketName))

  override def moveObject(implicit
    trace: Trace
  ): ZSink[Any, S3Connector.S3Exception, S3Connector.MoveObject, S3Connector.MoveObject, Unit] =
    ZSink.foreach(moveObject => s3.moveObject(moveObject))

  override def putObject(bucketName: => String, key: String)(implicit
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
        a <- TRef.make(Map.empty[String, S3Bucket])
      } yield TestS3Connector(TestS3(a))
    })

  private[s3] sealed trait S3Node {
    def id: String
  }

  private[s3] object S3Node {
    final case class S3Bucket(id: String, objects: Map[String, S3Obj]) extends S3Node

    final case class S3Obj(id: String, content: Chunk[Byte]) extends S3Node
  }

  private[s3] final case class TestS3(repo: TRef[Map[String, S3Bucket]]) {

    private def bucketDoesntExistException(name: String): S3Exception =
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
                 destinationBucket.id,
                 S3Bucket(destinationBucket.id, destinationBucket.objects.updated(m.objectKey, sourceObject))
               )
             )
      } yield ())

    def createBucket(name: String): ZIO[Any, S3Exception, Unit] =
      ZSTM.atomically(
        for {
          bucket <- repo.get.map(_.get(name))
          _ <- if (bucket.isDefined) ZSTM.succeed(())
               else repo.getAndUpdate(m => m.updated(name, S3Bucket(name, Map.empty)))
        } yield ()
      )

    def deleteEmptyBucket(bucket: String): ZIO[Any, S3Exception, Unit] =
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

    def deleteObject(bucket: String, key: String): ZIO[Any, S3Exception, Unit] =
      ZSTM.atomically(
        for {
          map <- repo.get
          bucket <-
            ZSTM
              .fromOption(map.get(bucket))
              .mapError(_ => bucketDoesntExistException(bucket))
          _ <- repo.set(map.updated(bucket.id, S3Bucket(bucket.id, bucket.objects - key)))
        } yield ()
      )

    def getObject(bucketName: String, key: String): ZStream[Any, S3Exception, Byte] =
      ZStream.unwrap(
        ZSTM.atomically(
          for {
            map    <- repo.get
            bucket <- ZSTM.fromOption(map.get(bucketName)).orElseFail(bucketDoesntExistException(bucketName))
            obj    <- ZSTM.fromOption(bucket.objects.get(key)).orElseFail(objectDoesntExistException(key))
          } yield ZStream.fromChunk(obj.content)
        )
      )

    def listBuckets: ZIO[Any, S3Exception, Chunk[String]] =
      ZSTM.atomically(
        repo.get.map(_.keys).map(Chunk.fromIterable)
      )

    def listObjects(bucketName: String): ZIO[Any, S3Exception, Chunk[String]] =
      ZSTM.atomically(
        for {
          map <- repo.get
          bucket <- ZSTM
                      .fromOption(map.get(bucketName))
                      .mapError(_ => S3Exception(NoSuchBucketException.builder().build()))
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
                 destinationBucket.id,
                 S3Bucket(destinationBucket.id, destinationBucket.objects.updated(m.targetObjectKey, sourceObject))
               )
               m1.updated(sourceBucket.id, S3Bucket(sourceBucket.id, sourceBucket.objects - m.objectKey))
             }
      } yield ())

    private def objectDoesntExistException(name: String): S3Exception =
      S3Exception(new RuntimeException(s"Object $name doesn't exist"))

    def putObject(
      bucketName: String,
      key: String,
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
