package zio.connect.s3

import zio.aws.s3.model.primitives.{BucketName, ObjectKey}

object S3Connector {

  final case class CopyObject(
    sourceBucketName: BucketName,
    objectKey: ObjectKey,
    targetBucketName: BucketName
  )

  final case class MoveObject(
    bucketName: BucketName,
    objectKey: ObjectKey,
    targetBucketName: BucketName,
    targetObjectKey: ObjectKey
  )

}
