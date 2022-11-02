package zio.connect.awslambda

import zio.Chunk
import zio.connect.awslambda.AwsLambdaConnector._
import zio.stream.ZStream
import zio.test._

trait AwsLambdaConnectorSpec extends ZIOSpecDefault {

  val awsLambdaConnectorSpec = invokeLambdaSpec

  lazy val invokeLambdaSpec =
    suite("invoke")(
      test("succeeds") {

        for {
          _ <- ZStream(
                 AwsLambdaConnector.CreateFunctionRequest(
                   FunctionName(""),
                   RoleArn(""),
                   FunctionCode(zipFile = Some(Blob(Chunk[Byte](1.toByte))))
                 )
               ) >>> createFunction
        } yield assertTrue(true)

      }
    )

}
