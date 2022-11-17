package zio.connect.awslambda

import zio.aws.core.AwsError
import zio.aws.lambda.Lambda
import zio.aws.lambda.model._
import zio.stream._
import zio.{Chunk, Trace, ZIO, ZLayer}

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
    Chunk[GetFunctionConcurrencyResponse]
  ] =
    ZSink
      .foldLeftZIO[Any, AwsError, GetFunctionConcurrencyRequest, Chunk[GetFunctionConcurrencyResponse]](
        Chunk.empty[GetFunctionConcurrencyResponse]
      )((s, m) => lambda.getFunctionConcurrency(m).map(_.asEditable).map(a => s :+ a))

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

  override def listTags(m: => ListTagsRequest)(implicit trace: Trace): ZStream[Any, AwsError, ListTagsResponse] =
    ZStream.fromZIO(lambda.listTags(m).map(_.asEditable))

  override def tagResource(implicit trace: Trace): ZSink[Any, AwsError, TagResourceRequest, TagResourceRequest, Unit] =
    ZSink.foreach(m => lambda.tagResource(m))

  override def untagResource(implicit
    trace: Trace
  ): ZSink[Any, AwsError, UntagResourceRequest, UntagResourceRequest, Unit] =
    ZSink.foreach(m => lambda.untagResource(m))

}

object LiveAwsLambdaConnector {

  val layer: ZLayer[Lambda, Nothing, LiveAwsLambdaConnector] =
    ZLayer.fromZIO(ZIO.service[Lambda].map(LiveAwsLambdaConnector(_)))

}
