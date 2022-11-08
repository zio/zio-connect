package zio.connect.awslambda

import zio.{Chunk, Trace}
import zio.aws.core.AwsError
import zio.aws.lambda.model.{
  CreateFunctionRequest,
  CreateFunctionResponse,
  CreateFunctionUrlConfigRequest,
  CreateFunctionUrlConfigResponse,
  InvokeRequest,
  InvokeResponse
}
import zio.stream.ZSink

trait AwsLambdaConnector {

  def createFunction(implicit
    trace: Trace
  ): ZSink[Any, AwsError, CreateFunctionRequest, Nothing, Chunk[CreateFunctionResponse]]

  def createFunctionUrlConfig(implicit
    trace: Trace
  ): ZSink[
    Any,
    AwsError,
    CreateFunctionUrlConfigRequest,
    Nothing,
    Chunk[CreateFunctionUrlConfigResponse]
  ]

  def invoke(implicit trace: Trace): ZSink[Any, AwsError, InvokeRequest, Nothing, Chunk[InvokeResponse]]

}

object AwsLambdaConnector {

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
