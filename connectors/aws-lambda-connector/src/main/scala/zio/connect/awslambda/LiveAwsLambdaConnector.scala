package zio.connect.awslambda

import zio.{Chunk, Trace, ZIO, ZLayer}
import zio.aws.core.AwsError
import zio.aws.lambda.Lambda
import zio.stream._
import zio.aws.lambda.model._

case class LiveAwsLambdaConnector(lambda: Lambda) extends AwsLambdaConnector {

  override def createAlias(implicit
    trace: Trace
  ): ZSink[Any, AwsError, CreateAliasRequest, CreateAliasRequest, Chunk[CreateAliasResponse]] =
    ZSink
      .foldLeftZIO[Any, AwsError, CreateAliasRequest, Chunk[CreateAliasResponse]](
        Chunk.empty[CreateAliasResponse]
      )((s, m) => lambda.createAlias(m).map(_.asEditable).map(a => s :+ a))

  override def createFunction(implicit
    trace: Trace
  ): ZSink[Any, AwsError, CreateFunctionRequest, CreateFunctionRequest, Chunk[CreateFunctionResponse]] =
    ZSink
      .foldLeftZIO[Any, AwsError, CreateFunctionRequest, Chunk[CreateFunctionResponse]](
        Chunk.empty[CreateFunctionResponse]
      )((s, m) => lambda.createFunction(m).map(_.asEditable).map(a => s :+ a))

  override def createFunctionUrlConfig(implicit
    trace: Trace
  ): ZSink[
    Any,
    AwsError,
    CreateFunctionUrlConfigRequest,
    CreateFunctionUrlConfigRequest,
    Chunk[CreateFunctionUrlConfigResponse]
  ] =
    ZSink
      .foldLeftZIO[Any, AwsError, CreateFunctionUrlConfigRequest, Chunk[CreateFunctionUrlConfigResponse]](
        Chunk.empty[CreateFunctionUrlConfigResponse]
      )((s, m) => lambda.createFunctionUrlConfig(m).map(_.asEditable).map(a => s :+ a))

  override def deleteAlias(implicit trace: Trace): ZSink[Any, AwsError, DeleteAliasRequest, DeleteAliasRequest, Unit] =
    ZSink.foreach[Any, AwsError, DeleteAliasRequest](m => lambda.deleteAlias(m))

  override def deleteFunction(implicit
    trace: Trace
  ): ZSink[Any, AwsError, DeleteFunctionRequest, DeleteFunctionRequest, Unit] =
    ZSink.foreach[Any, AwsError, DeleteFunctionRequest](m => lambda.deleteFunction(m))

  override def getAlias(implicit
    trace: Trace
  ): ZSink[Any, AwsError, GetAliasRequest, GetAliasRequest, Chunk[GetAliasResponse]] =
    ZSink
      .foldLeftZIO[Any, AwsError, GetAliasRequest, Chunk[GetAliasResponse]](Chunk.empty[GetAliasResponse])((s, m) =>
        lambda.getAlias(m).map(_.asEditable).map(a => s :+ a)
      )

  override def getFunction(implicit
    trace: Trace
  ): ZSink[Any, AwsError, GetFunctionRequest, GetFunctionRequest, Chunk[GetFunctionResponse]] =
    ZSink
      .foldLeftZIO[Any, AwsError, GetFunctionRequest, Chunk[GetFunctionResponse]](Chunk.empty[GetFunctionResponse])(
        (s, m) => lambda.getFunction(m).map(_.asEditable).map(a => s :+ a)
      )

  override def getFunctionConcurrency(implicit
    trace: Trace
  ): ZSink[
    Any,
    AwsError,
    GetFunctionConcurrencyRequest,
    GetFunctionConcurrencyRequest,
    GetFunctionConcurrencyResponse
  ] =
    ZSink
      .take[GetFunctionConcurrencyRequest](1)
      .map(_.head)
      .mapZIO(a => lambda.getFunctionConcurrency(a).map(_.asEditable))

  override def invoke(implicit
    trace: Trace
  ): ZSink[Any, AwsError, InvokeRequest, InvokeRequest, Chunk[InvokeResponse]] =
    ZSink
      .foldLeftZIO[Any, AwsError, InvokeRequest, Chunk[InvokeResponse]](Chunk.empty[InvokeResponse])((s, m) =>
        lambda.invoke(m).map(_.asEditable).map(a => s :+ a)
      )

  override def listAliases(m: => ListAliasesRequest)(implicit
    trace: Trace
  ): ZStream[Any, AwsError, AliasConfiguration] =
    lambda.listAliases(m).map(_.asEditable)

  override def listFunctions(m: => ListFunctionsRequest)(implicit
    trace: Trace
  ): ZStream[Any, AwsError, FunctionConfiguration] =
    lambda.listFunctions(m).map(_.asEditable)

  override def listTags(m: ListTagsRequest)(implicit trace: Trace): ZStream[Any, AwsError, ListTagsResponse] =
    ZStream.fromZIO(lambda.listTags(m).map(_.asEditable))

}

object LiveAwsLambdaConnector {

  val layer: ZLayer[Lambda, Nothing, LiveAwsLambdaConnector] =
    ZLayer.fromZIO(ZIO.service[Lambda].map(LiveAwsLambdaConnector(_)))

}
