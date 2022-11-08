package zio.connect

import zio.{Chunk, Trace}
import zio.aws.core.AwsError
import zio.aws.lambda.model.{CreateFunctionRequest, CreateFunctionResponse, InvokeRequest, InvokeResponse}
import zio.stream.ZSink

package object awslambda {

  val awsLambdaConnectorLiveLayer = LiveAwsLambdaConnector.layer

  def createFunction(implicit
    trace: Trace
  ): ZSink[AwsLambdaConnector, AwsError, CreateFunctionRequest, Nothing, Chunk[CreateFunctionResponse]] =
    ZSink.serviceWithSink(_.createFunction)

  def invoke(implicit
    trace: Trace
  ): ZSink[AwsLambdaConnector, AwsError, InvokeRequest, Nothing, Chunk[InvokeResponse]] =
    ZSink.serviceWithSink(_.invoke)

}
