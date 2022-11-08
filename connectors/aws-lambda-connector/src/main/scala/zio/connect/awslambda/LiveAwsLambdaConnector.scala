package zio.connect.awslambda

import zio.{Chunk, Trace, ZIO, ZLayer}
import zio.aws.core.AwsError
import zio.aws.lambda.Lambda
import zio.stream.ZSink
import zio.aws.lambda.model.{
  CreateFunctionRequest,
  CreateFunctionResponse,
  CreateFunctionUrlConfigRequest,
  CreateFunctionUrlConfigResponse,
  InvokeRequest,
  InvokeResponse
}

case class LiveAwsLambdaConnector(lambda: Lambda) extends AwsLambdaConnector {

  override def createFunction(implicit
    trace: Trace
  ): ZSink[Any, AwsError, CreateFunctionRequest, Nothing, Chunk[CreateFunctionResponse]] =
    ZSink
      .foldLeftZIO[Any, AwsError, CreateFunctionRequest, Chunk[CreateFunctionResponse]](
        Chunk.empty[CreateFunctionResponse]
      )((s, m) => lambda.createFunction(m).map(_.asEditable).map(a => s :+ a))
      .ignoreLeftover

  override def createFunctionUrlConfig(implicit
    trace: Trace
  ): ZSink[
    Any,
    AwsError,
    CreateFunctionUrlConfigRequest,
    Nothing,
    Chunk[CreateFunctionUrlConfigResponse]
  ] =
    ZSink
      .foldLeftZIO[Any, AwsError, CreateFunctionUrlConfigRequest, Chunk[CreateFunctionUrlConfigResponse]](
        Chunk.empty[CreateFunctionUrlConfigResponse]
      )((s, m) => lambda.createFunctionUrlConfig(m).map(_.asEditable).map(a => s :+ a))
      .ignoreLeftover

  override def invoke(implicit trace: Trace): ZSink[Any, AwsError, InvokeRequest, Nothing, Chunk[InvokeResponse]] =
    ZSink
      .foldLeftZIO[Any, AwsError, InvokeRequest, Chunk[InvokeResponse]](Chunk.empty[InvokeResponse])((s, m) =>
        lambda.invoke(m).map(_.asEditable).map(a => s :+ a)
      )
      .ignoreLeftover
}

object LiveAwsLambdaConnector {

  val layer: ZLayer[Lambda, Nothing, LiveAwsLambdaConnector] =
    ZLayer.fromZIO(ZIO.service[Lambda].map(LiveAwsLambdaConnector(_)))

}
