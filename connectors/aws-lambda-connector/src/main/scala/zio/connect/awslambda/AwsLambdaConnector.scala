package zio.connect.awslambda

import zio.{Chunk, Trace}
import zio.connect.awslambda.AwsLambdaConnector._
import zio.prelude.Subtype
import zio.prelude.data.Optional
import zio.stream.ZSink

trait AwsLambdaConnector {

  def createFunction(implicit
    trace: Trace
  ): ZSink[Any, AwsLambdaException, CreateFunctionRequest, CreateFunctionRequest, Unit]

  def invoke(implicit trace: Trace): ZSink[Any, AwsLambdaException, InvokeRequest, InvokeRequest, Unit]

}

object AwsLambdaConnector {

  final case class AwsLambdaException(exception: Throwable)

  object FunctionName extends Subtype[String]
  type FunctionName = FunctionName.Type

  object NamespacedFunctionName extends Subtype[String]
  type NamespacedFunctionName = NamespacedFunctionName.Type

  object RoleArn extends Subtype[String]
  type RoleArn = RoleArn.Type

  object Blob extends Subtype[Chunk[Byte]]
  type Blob = Blob.Type

  object S3Bucket extends Subtype[String]
  type S3Bucket = S3Bucket.Type

  object S3Key extends Subtype[String]
  type S3Key = S3Key.Type

  object S3ObjectVersion extends Subtype[String]
  type S3ObjectVersion = S3ObjectVersion.Type

  final case class FunctionCode(
    zipFile: Optional[Blob] = Optional.Absent,
    s3Bucket: Optional[S3Bucket] = Optional.Absent,
    s3Key: Optional[S3Key] = Optional.Absent,
    s3ObjectVersion: Optional[S3ObjectVersion] = Optional.Absent,
    imageUri: Optional[String] = Optional.Absent
  )

  final case class CreateFunctionRequest(
    functionName: FunctionName,
    role: RoleArn,
    code: FunctionCode
  )

  final case class InvokeRequest(
    functionName: NamespacedFunctionName,
    payload: Optional[Blob],
    invocationType: InvocationType
  )

  sealed trait InvocationType
  object InvocationType {
    final case object UNKNOWN_TO_SDK_VERSION extends InvocationType
    final case object EVENT                  extends InvocationType
    final case object REQUEST_RESPONSE       extends InvocationType
    final case object DRY_RUN                extends InvocationType
  }

//  def invoke(invokeRequest: AwsInvokeRequest): ZIO[Lambda, AwsError, AwsInvokeResponse.ReadOnly] =
//    zio.aws.lambda.Lambda.invoke(invokeRequest)

//  zio.aws.lambda.Lambda.createFunction(cfr)
//  zio.aws.lambda.Lambda.invoke(cfr)
//
//  zio.aws.lambda.Lambda.getFunction()
//  zio.aws.lambda.Lambda.deleteFunction()
//  zio.aws.lambda.Lambda.listFunctions()
//  zio.aws.lambda.Lambda.deleteAlias()
//  zio.aws.lambda.Lambda.getAlias()
//  zio.aws.lambda.Lambda.listAliases()
//  zio.aws.lambda.Lambda.getFunctionConcurrency()
//  zio.aws.lambda.Lambda.listTags()

}
