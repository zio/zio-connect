package zio.connect

import zio.{Chunk, Trace}
import zio.aws.core.AwsError
import zio.aws.lambda.model._
import zio.stream.{ZSink, ZStream}

package object awslambda {

  val awsLambdaConnectorLiveLayer = LiveAwsLambdaConnector.layer

  def createAlias(implicit
    trace: Trace
  ): ZSink[AwsLambdaConnector, AwsError, CreateAliasRequest, CreateAliasRequest, Chunk[CreateAliasResponse]] =
    ZSink.serviceWithSink(_.createAlias)

  def createFunction(implicit
    trace: Trace
  ): ZSink[AwsLambdaConnector, AwsError, CreateFunctionRequest, CreateFunctionRequest, Chunk[CreateFunctionResponse]] =
    ZSink.serviceWithSink(_.createFunction)

  def createFunctionUrlConfig(implicit
    trace: Trace
  ): ZSink[AwsLambdaConnector, AwsError, CreateFunctionUrlConfigRequest, CreateFunctionUrlConfigRequest, Chunk[
    CreateFunctionUrlConfigResponse
  ]] =
    ZSink.serviceWithSink(_.createFunctionUrlConfig)

  def deleteAlias(implicit
    trace: Trace
  ): ZSink[AwsLambdaConnector, AwsError, DeleteAliasRequest, DeleteAliasRequest, Unit] =
    ZSink.serviceWithSink(_.deleteAlias)

  def deleteFunction(implicit
    trace: Trace
  ): ZSink[AwsLambdaConnector, AwsError, DeleteFunctionRequest, DeleteFunctionRequest, Unit] =
    ZSink.serviceWithSink(_.deleteFunction)

  def getAlias(implicit
    trace: Trace
  ): ZSink[AwsLambdaConnector, AwsError, GetAliasRequest, GetAliasRequest, Chunk[GetAliasResponse]] =
    ZSink.serviceWithSink(_.getAlias)

  def getFunction(implicit
    trace: Trace
  ): ZSink[AwsLambdaConnector, AwsError, GetFunctionRequest, GetFunctionRequest, Chunk[GetFunctionResponse]] =
    ZSink.serviceWithSink(_.getFunction)

  def getFunctionConcurrency(implicit
    trace: Trace
  ): ZSink[
    AwsLambdaConnector,
    AwsError,
    GetFunctionConcurrencyRequest,
    GetFunctionConcurrencyRequest,
    GetFunctionConcurrencyResponse
  ] = ZSink.serviceWithSink(_.getFunctionConcurrency)

  def invoke(implicit
    trace: Trace
  ): ZSink[AwsLambdaConnector, AwsError, InvokeRequest, InvokeRequest, Chunk[InvokeResponse]] =
    ZSink.serviceWithSink(_.invoke)

  def listAliases(m: => ListAliasesRequest)(implicit
    trace: Trace
  ): ZStream[AwsLambdaConnector, AwsError, AliasConfiguration] =
    ZStream.serviceWithStream(_.listAliases(m))

  def listFunctions(m: => ListFunctionsRequest)(implicit
    trace: Trace
  ): ZStream[AwsLambdaConnector, AwsError, FunctionConfiguration] =
    ZStream.serviceWithStream(_.listFunctions(m))

  def listTags(m: ListTagsRequest)(implicit trace: Trace): ZStream[AwsLambdaConnector, AwsError, ListTagsResponse] =
    ZStream.serviceWithStream(_.listTags(m))

}
