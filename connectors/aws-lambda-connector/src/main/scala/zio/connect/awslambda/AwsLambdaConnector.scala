package zio.connect.awslambda

import zio.{Chunk, Trace}
import zio.aws.core.AwsError
import zio.aws.lambda.model._
import zio.stream._

trait AwsLambdaConnector {

  def createAlias(implicit
    trace: Trace
  ): ZSink[Any, AwsError, CreateAliasRequest, CreateAliasRequest, Chunk[CreateAliasResponse]]

  def createFunction(implicit
    trace: Trace
  ): ZSink[Any, AwsError, CreateFunctionRequest, CreateFunctionRequest, Chunk[CreateFunctionResponse]]

  def deleteAlias(implicit trace: Trace): ZSink[Any, AwsError, DeleteAliasRequest, DeleteAliasRequest, Unit]

  def deleteFunction(implicit trace: Trace): ZSink[Any, AwsError, DeleteFunctionRequest, DeleteFunctionRequest, Unit]

  def getAlias(implicit trace: Trace): ZSink[Any, AwsError, GetAliasRequest, GetAliasRequest, Chunk[GetAliasResponse]]

  def getFunction(implicit
    trace: Trace
  ): ZSink[Any, AwsError, GetFunctionRequest, GetFunctionRequest, Chunk[GetFunctionResponse]]

  def getFunctionConcurrency(implicit
    trace: Trace
  ): ZSink[
    Any,
    AwsError,
    GetFunctionConcurrencyRequest,
    GetFunctionConcurrencyRequest,
    Chunk[GetFunctionConcurrencyResponse]
  ]

  def invoke(implicit trace: Trace): ZSink[Any, AwsError, InvokeRequest, InvokeRequest, Chunk[InvokeResponse]]

  def listAliases(m: => ListAliasesRequest)(implicit
    trace: Trace
  ): ZStream[Any, AwsError, AliasConfiguration]

  def listFunctions(m: => ListFunctionsRequest)(implicit
    trace: Trace
  ): ZStream[Any, AwsError, FunctionConfiguration]

  def listTags(m: => ListTagsRequest)(implicit trace: Trace): ZStream[Any, AwsError, ListTagsResponse]

  def tagResource(implicit trace: Trace): ZSink[Any, AwsError, TagResourceRequest, TagResourceRequest, Unit]

  def untagResource(implicit
    trace: Trace
  ): ZSink[Any, AwsError, UntagResourceRequest, UntagResourceRequest, Unit]

}
