package zio.connect.awslambda

import zio.Chunk
import zio.aws.lambda.model.{CreateFunctionRequest, FunctionCode, InvokeRequest}
import zio.aws.lambda.model.primitives.{Blob, FunctionName, Handler, NamespacedFunctionName, RoleArn}
import zio.stream.{ZPipeline, ZStream}
import zio.test._
import zio.test.Assertion._

import java.util.UUID

trait AwsLambdaConnectorSpec extends ZIOSpecDefault {

  val awsLambdaConnectorSpec = invokeLambdaSpec

  lazy val invokeLambdaSpec =
    suite("invoke")(
      test("succeeds") {

        for {
          zipFile     <- ZStream.fromFileURI(this.getClass.getResource("/handler.js.zip").toURI).runCollect
          functionName = "myCustomFunction"
          _ <- ZStream(
                 CreateFunctionRequest(
                   functionName = FunctionName(functionName),
                   runtime = Some(zio.aws.lambda.model.Runtime.`nodejs14.x`),
                   role = RoleArn("cool-stacklifter"),
                   handler = Some(Handler("handler.handler")),
                   code = FunctionCode(zipFile = Some(Blob(zipFile)))
                 )
               ) >>> createFunction
          payload1 = s"""{"value":"${UUID.randomUUID().toString}"}"""
          payload2 = s"""{"value":"${UUID.randomUUID().toString}"}"""
          payload3 = s"""{"value":"${UUID.randomUUID().toString}"}"""
          createInvokeRequest = (payload: String) =>
                                  InvokeRequest(
                                    functionName = NamespacedFunctionName(functionName),
                                    payload = Some(Blob(Chunk.fromIterable(payload.getBytes)))
                                  )
          response <- ZStream(
                        createInvokeRequest(payload1),
                        createInvokeRequest(payload2),
                        createInvokeRequest(payload3)
                      ) >>> zio.connect.awslambda.invoke
          payloads <- ZStream
                        .fromIterable(response.flatMap(_.payload.toList).flatMap(b => b.toList))
                        .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
                        .runCollect
        } yield assert(payloads.sorted)(equalTo(Chunk(payload1, payload2, payload3).sorted))

      }
    )

}
