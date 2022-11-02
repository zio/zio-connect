package zio.connect

import zio.Trace
import zio.connect.awslambda.AwsLambdaConnector.{AwsLambdaException, CreateFunctionRequest, InvokeRequest}
import zio.stream.ZSink

package object awslambda {

  val awsLambdaConnectorLiveLayer = LiveAwsLambdaConnector.layer

  def createFunction(implicit
    trace: Trace
  ): ZSink[AwsLambdaConnector, AwsLambdaException, CreateFunctionRequest, CreateFunctionRequest, Unit] =
    ZSink.serviceWithSink(_.createFunction)

  def invoke(implicit trace: Trace): ZSink[AwsLambdaConnector, AwsLambdaException, InvokeRequest, InvokeRequest, Unit] =
    ZSink.serviceWithSink(_.invoke)

}
