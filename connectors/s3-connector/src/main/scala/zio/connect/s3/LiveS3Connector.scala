package zio.connect.s3
import zio.{Trace, ZIO, ZLayer}
import zio.aws.s3.S3
import zio.aws.s3.model.{CreateBucketRequest, DeleteBucketRequest}
import zio.aws.s3.model.primitives.BucketName
import zio.connect.s3.S3Connector.S3Exception
import zio.stream.{ZSink, ZStream}

case class LiveS3Connector(s3: S3) extends S3Connector {

  override def createBucket(implicit trace: Trace): ZSink[Any, S3Exception, String, Nothing, Unit] =
    ZSink.foreach { name =>
      s3.createBucket(CreateBucketRequest(bucket = BucketName(name)))
    }

  override def deleteEmptyBucket(implicit trace: Trace): ZSink[Any, S3Exception, String, Nothing, Unit] =
    ZSink.foreach { name =>
      s3.deleteBucket(DeleteBucketRequest(bucket = BucketName(name)))
    }

  override def deleteObject(bucketName: => String, keys: Iterable[String])(implicit
    trace: Trace
  ): ZSink[Any, S3Exception, S3Connector.ObjectId, Nothing, Unit] = ???

  override def existsBucket(name: => String)(implicit trace: Trace): ZStream[Any, S3Exception, Boolean] =
    ZStream.fromZIO(
      for {
        listResponse <- s3.listBuckets().flatMap(_.getBuckets)
        bucketExists  = listResponse.exists(_.name.contains(name))
      } yield bucketExists
    )

  override def getObject(bucketName: => String, key: String)(implicit trace: Trace): ZStream[Any, S3Exception, Byte] =
    ???

  override def listObjects(bucketName: => String)(implicit
    trace: Trace
  ): ZStream[Any, S3Exception, S3Connector.ObjectId] = ???

  override def putObject(bucketName: => String, key: String)(implicit
    trace: Trace
  ): ZSink[Any, S3Exception, Byte, Nothing, Unit] = ???
}

object LiveS3Connector {

  val live: ZLayer[S3, Throwable, LiveS3Connector] =
    ZLayer.fromZIO(ZIO.service[S3].map(LiveS3Connector(_)))

}
