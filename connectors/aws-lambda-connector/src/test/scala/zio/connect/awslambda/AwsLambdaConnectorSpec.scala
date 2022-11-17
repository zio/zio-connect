package zio.connect.awslambda

import zio.aws.core.{AwsError, GenericAwsError}
import zio.aws.lambda.model._
import zio.aws.lambda.model.primitives._
import zio.stream.{ZPipeline, ZStream}
import zio.test.Assertion._
import zio.test._
import zio.{Chunk, ZIO}

import java.util.UUID

trait AwsLambdaConnectorSpec extends ZIOSpecDefault {

  val awsLambdaConnectorSpec: Spec[AwsLambdaConnector, Object] =
    createAliasSpec + invokeLambdaSpec + listFunctionsSpec + tagResourceSpec

  private lazy val createAliasSpec =
    suite("createAlias")(
      test("succeeds") {
        val alias1 = Alias("alias1")
        val alias2 = Alias("alias2")
        for {
          zipFile     <- ZStream.fromFileURI(this.getClass.getResource("/handler.js.zip").toURI).runCollect
          functionName = FunctionName(UUID.randomUUID().toString)
          createFunctionResponse <-
            ZStream(
              CreateFunctionRequest(
                functionName = FunctionName(functionName),
                runtime = Some(zio.aws.lambda.model.Runtime.`nodejs14.x`),
                role = RoleArn("cool-stacklifter"),
                handler = Some(Handler("handler.handler")),
                code = FunctionCode(zipFile = Some(Blob(zipFile)))
              )
            ) >>> createFunction
          functionVersion <-
            ZIO
              .fromOption(
                createFunctionResponse
                  .find(_.functionName.contains(functionName))
                  .flatMap(_.version.toOption)
              )
              .orElseFail(new RuntimeException("No functionVersion in response"))
              .orDie
          createdAliases1 <- ZStream(
                               CreateAliasRequest(functionName, alias1, functionVersion)
                             ) >>> createAlias
          createdAliases2 <- ZStream(
                               CreateAliasRequest(functionName, alias2, functionVersion)
                             ) >>> createAlias
          createdAliases = createdAliases1 ++ createdAliases2
          listedAliases <- listAliases(ListAliasesRequest(functionName)).runCollect.map(_.map(_.name.toChunk).flatten)

          _ <- ZStream(DeleteAliasRequest(functionName, alias2)) >>> deleteAlias
          remainingAliases <-
            listAliases(ListAliasesRequest(functionName)).runCollect.map(_.map(_.name.toChunk).flatten)

          getRemainingAliasResult <- ZStream(GetAliasRequest(functionName, alias1)) >>> getAlias
          getDeletedAliasResult <- (ZStream(GetAliasRequest(functionName, alias2)) >>> getAlias)
                                     .catchSome[AwsLambdaConnector, AwsError, Chunk[GetAliasResponse]] {
                                       case GenericAwsError(reason)
                                           if reason.getClass.getSimpleName == "ResourceNotFoundException" =>
                                         ZIO.succeed(Chunk.empty[GetAliasResponse])
                                     }
        } yield assertTrue(
          createdAliases.map(_.name.toChunk).flatten.sortBy(_.toString) == listedAliases.sortBy(_.toString)
        ) && assertTrue(
          remainingAliases.contains(alias1)
        ) && assertTrue(getDeletedAliasResult.isEmpty) && assertTrue(
          getRemainingAliasResult.map(_.name.toChunk).flatten.contains(alias1)
        )

      }
    )

  private lazy val invokeLambdaSpec =
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
          invokeResponses <- ZStream(
                               createInvokeRequest(payload1),
                               createInvokeRequest(payload2),
                               createInvokeRequest(payload3)
                             ) >>> zio.connect.awslambda.invoke
          functionConcurrency <-
            ZStream(GetFunctionConcurrencyRequest(FunctionName(functionName))) >>> getFunctionConcurrency
          invokeResponsesPayloads <- ZStream
                                       .fromIterable(invokeResponses.flatMap(_.payload.toList).flatMap(b => b.toList))
                                       .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
                                       .runCollect
        } yield assert(invokeResponsesPayloads.sorted)(
          equalTo(Chunk(payload1, payload2, payload3).sorted)
        ) && assertTrue(functionConcurrency.size == 1)
      }
    )

  private lazy val listFunctionsSpec =
    suite("listFunctions")(
      test("succeeds") {
        for {
          zipFile      <- ZStream.fromFileURI(this.getClass.getResource("/handler.js.zip").toURI).runCollect
          functionName1 = FunctionName(UUID.randomUUID().toString)
          functionName2 = FunctionName(UUID.randomUUID().toString)
          _ <- ZStream(
                 CreateFunctionRequest(
                   functionName = FunctionName(functionName1),
                   runtime = Some(zio.aws.lambda.model.Runtime.`nodejs14.x`),
                   role = RoleArn("cool-stacklifter"),
                   handler = Some(Handler("handler.handler")),
                   code = FunctionCode(zipFile = Some(Blob(zipFile)))
                 ),
                 CreateFunctionRequest(
                   functionName = FunctionName(functionName2),
                   runtime = Some(zio.aws.lambda.model.Runtime.`nodejs14.x`),
                   role = RoleArn("cool-stacklifter"),
                   handler = Some(Handler("handler.handler")),
                   code = FunctionCode(zipFile = Some(Blob(zipFile)))
                 )
               ) >>> createFunction
          functions <- listFunctions(ListFunctionsRequest()).runCollect
          functionNames = functions
                            .map(_.functionName.toChunk)
                            .flatten
                            .toList

          _                      <- ZStream(DeleteFunctionRequest(functionName2)) >>> deleteFunction
          functionsAfterDeletion <- listFunctions(ListFunctionsRequest()).runCollect
          functionNamesAfterDeletion = functionsAfterDeletion
                                         .map(_.functionName.toChunk)
                                         .flatten
                                         .toList

          getDeletedFunctionResult <-
            (ZStream(GetFunctionRequest(NamespacedFunctionName(functionName1))) >>> getFunction)
              .catchSome[AwsLambdaConnector, AwsError, Chunk[GetFunctionResponse]] {
                case GenericAwsError(reason) if reason.getClass.getSimpleName == "ResourceNotFoundException" =>
                  ZIO.succeed(Chunk.empty[GetFunctionResponse])
              }
        } yield assertTrue(functionNames.contains(functionName1.toString)) && assertTrue(
          functionNames.contains(functionName2.toString)
        ) && assertTrue(functionNamesAfterDeletion.contains(functionName1.toString)) && assertTrue(
          !functionNamesAfterDeletion.contains(functionName2.toString)
        ) && assertTrue(
          getDeletedFunctionResult
            .map(_.configuration.flatMap(_.functionName).toChunk)
            .flatten
            .contains(functionName1.toString)
        )

      }
    )

  private lazy val tagResourceSpec = {
    suite("tagResource")(
      test("succeeds") {
        for {
          zipFile      <- ZStream.fromFileURI(this.getClass.getResource("/handler.js.zip").toURI).runCollect
          functionName1 = FunctionName(UUID.randomUUID().toString)
          functions <- ZStream(
                         CreateFunctionRequest(
                           functionName = FunctionName(functionName1),
                           runtime = Some(zio.aws.lambda.model.Runtime.`nodejs14.x`),
                           role = RoleArn("cool-stacklifter"),
                           handler = Some(Handler("handler.handler")),
                           code = FunctionCode(zipFile = Some(Blob(zipFile)))
                         )
                       ) >>> createFunction
          functionArn <-
            ZIO
              .fromOption(functions.find(a => a.functionName.contains(functionName1)).flatMap(_.functionArn.toOption))
              .orElseFail(new RuntimeException("Function was not found"))
          getTagsAsList =
            (a: Chunk[ListTagsResponse]) =>
              a.map(_.tags.toChunk).flatten.map(a => Chunk.fromIterable(a.toList)).flatten.sortBy(_._1.toString)
          initialTags <- listTags(ListTagsRequest(FunctionArn(functionArn))).runCollect.map(getTagsAsList)

          tag1 = TagKey("tag1") -> TagValue("value1")
          tag2 = TagKey("tag2") -> TagValue("value2")
          _ <- ZStream(
                 TagResourceRequest(
                   FunctionArn(functionArn),
                   Map(tag1, tag2)
                 )
               ) >>> tagResource
          tagsAfterCreation <- listTags(ListTagsRequest(FunctionArn(functionArn))).runCollect.map(getTagsAsList)
          _                 <- ZStream(UntagResourceRequest(FunctionArn(functionArn), Chunk(tag2._1))) >>> untagResource
          tagsAfterRemoval  <- listTags(ListTagsRequest(FunctionArn(functionArn))).runCollect.map(getTagsAsList)

        } yield assertTrue(initialTags.isEmpty) && assertTrue(tagsAfterCreation == Chunk(tag1, tag2)) && assertTrue(
          tagsAfterRemoval == Chunk(tag1)
        )
      }
    )

  }

}
