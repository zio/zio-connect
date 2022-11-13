package zio.connect.s3.multiregion

import software.amazon.awssdk.regions.Region
import zio.aws.core.AwsError
import zio.aws.s3.model.primitives.{BucketName, ObjectKey}
import zio.connect.s3.S3Connector.{CopyObject, MoveObject}
import zio.connect.s3.multiregion.TestMultiRegionS3Connector.S3Node.{S3Bucket, S3Obj}
import zio.connect.s3.multiregion.TestMultiRegionS3Connector.TestS3
import zio.stm.{STM, TRef, ZSTM}
import zio.stream.{ZSink, ZStream}
import zio.{Chunk, Trace, ZIO, ZLayer}

private[s3] final case class TestMultiRegionS3Connector(s3: TestS3) extends MultiRegionS3Connector {

  override def copyObject(region: => Region)(implicit
    trace: Trace
  ): ZSink[Any, AwsError, CopyObject, CopyObject, Unit] =
    ZSink.foreach(copyObject => s3.copyObject(copyObject, region, region))

  override def createBucket(region: => Region)(implicit
    trace: Trace
  ): ZSink[Any, AwsError, BucketName, BucketName, Unit] =
    ZSink.foreach(name => s3.createBucket(name, region))

  override def deleteEmptyBucket(region: => Region)(implicit
    trace: Trace
  ): ZSink[Any, AwsError, BucketName, BucketName, Unit] =
    ZSink.foreach(bucket => s3.deleteEmptyBucket(bucket, region))

  override def deleteObjects(bucketName: => BucketName, region: => Region)(implicit
    trace: Trace
  ): ZSink[Any, AwsError, ObjectKey, ObjectKey, Unit] =
    ZSink.foreach(key => s3.deleteObject(bucketName, key, region))

  override def getObject(bucketName: => BucketName, key: => ObjectKey, region: => Region)(implicit
    trace: Trace
  ): ZStream[Any, AwsError, Byte] =
    s3.getObject(bucketName, key, region)

  override def listBuckets(region: => Region)(implicit trace: Trace): ZStream[Any, AwsError, BucketName] =
    ZStream.fromIterableZIO(s3.listBuckets)

  override def listObjects(bucketName: => BucketName, region: => Region)(implicit
    trace: Trace
  ): ZStream[Any, AwsError, ObjectKey] =
    ZStream.fromIterableZIO(s3.listObjects(bucketName, region))

  override def putObject(bucketName: => BucketName, key: => ObjectKey, region: => Region)(implicit
    trace: Trace
  ): ZSink[Any, AwsError, Byte, Nothing, Unit] =
    ZSink.unwrap(for {
      ref <- TRef.makeCommit(false)
      r = ZSink.foreachChunk[Any, AwsError, Byte] { bytes =>
            s3.putObject(bucketName, key, bytes, ref, region)
          }
    } yield r)
}

object TestMultiRegionS3Connector {

  val layer: ZLayer[Any, Nothing, TestMultiRegionS3Connector] =
    ZLayer.fromZIO(STM.atomically {
      for {
        a <- TRef.make(Map.empty[(BucketName, Region), S3Bucket])
      } yield TestMultiRegionS3Connector(TestS3(a))
    })

  private[s3] sealed trait S3Node

  private[s3] object S3Node {
    final case class S3Bucket(name: BucketName, objects: Map[ObjectKey, S3Obj]) extends S3Node

    final case class S3Obj(key: ObjectKey, content: Chunk[Byte]) extends S3Node
  }

  private[s3] final case class TestS3(repo: TRef[Map[(BucketName, Region), S3Bucket]]) {
    private def bucketDoesntExistException(name: BucketName): AwsError =
      AwsError.fromThrowable(new RuntimeException(s"Bucket $name doesn't exist"))

    def copyObject(m: CopyObject, sourceRegion: => Region, destinationRegion: => Region): ZIO[Any, AwsError, Unit] =
      ZSTM.atomically(for {
        map               <- repo.get
        sourceBucket      <- getBucket(map, m.sourceBucketName, sourceRegion)
        destinationBucket <- getBucket(map, m.targetBucketName, destinationRegion)
        sourceObject <-
          ZSTM.fromOption(sourceBucket.objects.get(m.objectKey)).mapError(_ => objectDoesntExistException(m.objectKey))
        _ <- repo.update(mp =>
               mp.updated(
                 (destinationBucket.name, destinationRegion),
                 S3Bucket(destinationBucket.name, destinationBucket.objects.updated(m.objectKey, sourceObject))
               )
             )
      } yield ())

    def createBucket(name: BucketName, region: Region): ZIO[Any, AwsError, Unit] =
      ZSTM.atomically(
        for {
          bucketAlreadyExists <- repo.get.map(m => m.keys.map(_._1).contains(name))
          _ <- if (bucketAlreadyExists) ZSTM.fail(AwsError.fromThrowable(new RuntimeException("Bucket already exists")))
               else repo.getAndUpdate(m => m.updated((name, region), S3Bucket(name, Map.empty)))
        } yield ()
      )

    def deleteEmptyBucket(bucket: BucketName, region: Region): ZIO[Any, AwsError, Unit] =
      ZSTM.atomically(
        for {
          bucketOp <- repo.get.map(_.get((bucket, region)))
          _ <-
            bucketOp match {
              case Some(b) =>
                if (b.objects.isEmpty) repo.getAndUpdate(m => m - ((bucket, region)))
                else ZSTM.fail(AwsError.fromThrowable(new RuntimeException("Bucket not empty")))
              case None =>
                ZSTM.fail(AwsError.fromThrowable(new RuntimeException("Bucket does not exist")))
            }
        } yield ()
      )

    def deleteObject(bucket: BucketName, key: ObjectKey, region: Region): ZIO[Any, AwsError, Unit] =
      ZSTM.atomically(
        for {
          map <- repo.get
          bucket <-
            ZSTM
              .fromOption(map.get((bucket, region)))
              .mapError(_ => bucketDoesntExistException(bucket))
          _ <- repo.set(map.updated((bucket.name, region), S3Bucket(bucket.name, bucket.objects - key)))
        } yield ()
      )

    private def getBucket(map: Map[(BucketName, Region), S3Bucket], bucketName: BucketName, region: Region) =
      ZSTM
        .fromOption(map.get((bucketName, region)))
        .mapError(_ => bucketDoesntExistException(bucketName))

    def getObject(bucketName: BucketName, key: ObjectKey, region: Region): ZStream[Any, AwsError, Byte] =
      ZStream.unwrap(
        ZSTM.atomically(
          for {
            map <- repo.get
            bucket <-
              ZSTM.fromOption(map.get((bucketName, region))).orElseFail(bucketDoesntExistException(bucketName))
            obj <- ZSTM.fromOption(bucket.objects.get(key)).orElseFail(objectDoesntExistException(key))
          } yield ZStream.fromChunk(obj.content)
        )
      )

    def listBuckets: ZIO[Any, AwsError, Chunk[BucketName]] =
      ZSTM.atomically(
        repo.get.map(_.keys).map(k => Chunk.fromIterable(k.map(_._1)))
      )

    def listObjects(bucketName: BucketName, region: Region): ZIO[Any, AwsError, Chunk[ObjectKey]] =
      ZSTM.atomically(
        for {
          map <- repo.get
          bucket <- ZSTM
                      .fromOption(map.get((bucketName, region)))
                      .mapError { _ =>
                        AwsError.fromThrowable(new RuntimeException("No such bucket!"))
                      }
        } yield Chunk.fromIterable(bucket.objects.keys)
      )

    def moveObject(m: MoveObject, sourceRegion: Region, destinationRegion: Region): ZIO[Any, AwsError, Unit] =
      ZSTM.atomically(for {
        map <- repo.get
        sourceBucket <-
          ZSTM
            .fromOption(map.get((m.bucketName, sourceRegion)))
            .mapError(_ => bucketDoesntExistException(m.bucketName))
        destinationBucket <-
          ZSTM
            .fromOption(map.get((m.targetBucketName, destinationRegion)))
            .mapError(_ => bucketDoesntExistException(m.targetBucketName))
        sourceObject <-
          ZSTM.fromOption(sourceBucket.objects.get(m.objectKey)).mapError(_ => objectDoesntExistException(m.objectKey))
        _ <- repo.update { mp =>
               val m1 = mp.updated(
                 (destinationBucket.name, destinationRegion),
                 S3Bucket(destinationBucket.name, destinationBucket.objects.updated(m.targetObjectKey, sourceObject))
               )
               m1.updated(
                 (sourceBucket.name, sourceRegion),
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
      append: TRef[Boolean],
      region: Region
    ): ZIO[Any, AwsError, Unit] =
      ZSTM.atomically(
        for {
          map       <- repo.get
          appending <- append.get
          bucket <- ZSTM
                      .fromOption(map.get((bucketName, region)))
                      .mapError(_ => bucketDoesntExistException(bucketName))
          updatedMap =
            if (appending)
              map.updated(
                (bucketName, region),
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
                (bucketName, region),
                S3Bucket(bucketName, bucket.objects.updated(key, S3Obj(key, bytes)))
              )
          _ <- repo.set(updatedMap)
          _ <- append.set(true)
        } yield ()
      )
  }

}
