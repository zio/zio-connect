package zio.connect.awslambda

import zio.{Trace, ZIO, ZLayer}
import zio.aws.core.AwsError
import zio.aws.lambda.Lambda
import zio.connect.awslambda.AwsLambdaConnector._
import zio.stream.ZSink
import zio.aws.lambda.model.{
  CreateFunctionRequest => AwsCreateFunctionRequest,
  FunctionCode => AwsFunctionCode,
  InvokeRequest => AwsInvokeRequest
}

case class LiveAwsLambdaConnector(lambda: zio.aws.lambda.Lambda) extends AwsLambdaConnector {

  def blobTransformer(b: AwsLambdaConnector.Blob): zio.aws.lambda.model.primitives.Blob =
    zio.aws.lambda.model.primitives.Blob(b)

  def toModelError(awsError: AwsError): AwsLambdaException =
    AwsLambdaException(awsError.toThrowable)

  def functionNameTransformer(a: FunctionName): zio.aws.lambda.model.primitives.FunctionName =
    zio.aws.lambda.model.primitives.FunctionName(a)

  def namespacedFunctionNameTransformer(
    a: NamespacedFunctionName
  ): zio.aws.lambda.model.primitives.NamespacedFunctionName =
    zio.aws.lambda.model.primitives.NamespacedFunctionName(a)

  def roleArnTransformer(a: RoleArn): zio.aws.lambda.model.primitives.RoleArn =
    zio.aws.lambda.model.primitives.RoleArn(a)

  def s3ObjectVersionTransformer(a: S3ObjectVersion): zio.aws.lambda.model.primitives.S3ObjectVersion =
    zio.aws.lambda.model.primitives.S3ObjectVersion(a)

  def s3BucketTransformer(a: S3Bucket): zio.aws.lambda.model.primitives.S3Bucket =
    zio.aws.lambda.model.primitives.S3Bucket(a)

  def s3KeyTransformer(a: S3Key): zio.aws.lambda.model.primitives.S3Key =
    zio.aws.lambda.model.primitives.S3Key(a)

  def invocationTypeTransformer(a: InvocationType): zio.aws.lambda.model.InvocationType =
    zio.aws.lambda.model.InvocationType
      .wrap(software.amazon.awssdk.services.lambda.model.InvocationType.fromValue(a.toString))

  def awsCreateFunctionRequestTransformer(m: CreateFunctionRequest): AwsCreateFunctionRequest =
    AwsCreateFunctionRequest(
      functionName = functionNameTransformer(m.functionName),
      role = roleArnTransformer(m.role),
      code = AwsFunctionCode(
        zipFile = m.code.zipFile.map(blobTransformer),
        s3Bucket = m.code.s3Bucket.map(s3BucketTransformer),
        s3Key = m.code.s3Key.map(s3KeyTransformer),
        s3ObjectVersion = m.code.s3ObjectVersion.map(s3ObjectVersionTransformer),
        imageUri = m.code.imageUri
      )
    )

  override def createFunction(implicit
    trace: Trace
  ): ZSink[Any, AwsLambdaException, CreateFunctionRequest, CreateFunctionRequest, Unit] =
    ZSink
      .foreach[Any, AwsLambdaException, CreateFunctionRequest] { m =>
        lambda
          .createFunction(awsCreateFunctionRequestTransformer(m))
          .mapError(toModelError)
      }

  override def invoke(implicit trace: Trace): ZSink[Any, AwsLambdaException, InvokeRequest, InvokeRequest, Unit] =
    ZSink.foreach[Any, AwsLambdaException, InvokeRequest] { m =>
      lambda
        .invoke(
          AwsInvokeRequest(
            functionName = namespacedFunctionNameTransformer(m.functionName),
            invocationType = invocationTypeTransformer(m.invocationType),
            //            logType = ???,
            //            clientContext = ???,
            payload = m.payload.map(blobTransformer)
            //            qualifier = ???
          )
        )
        .mapError(toModelError)
    }
}

object LiveAwsLambdaConnector {

  val layer: ZLayer[Lambda, Nothing, LiveAwsLambdaConnector] =
    ZLayer.fromZIO(ZIO.service[Lambda].map(LiveAwsLambdaConnector(_)))

}
