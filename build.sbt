import Dependencies._
import explicitdeps.ExplicitDepsPlugin.autoImport.moduleFilterRemoveValue

enablePlugins(ZioSbtCiPlugin)

inThisBuild(
  List(
    name              := "ZIO Connect",
    ciEnabledBranches := Seq("master"),
    developers := List(
      Developer(
        "jdegoes",
        "John De Goes",
        "john@degoes.net",
        url("http://degoes.net")
      )
    )
  )
)

addCommandAlias("fmt", "all scalafmtSbt scalafmt Test/scalafmt")
addCommandAlias("fix", "; all Compile/scalafix Test/scalafix; all scalafmtSbt scalafmtAll")
addCommandAlias("check", "; scalafmtSbtCheck; scalafmtCheckAll; Compile/scalafix --check; Test/scalafix --check")

lazy val root = project
  .in(file("."))
  .settings(
    publish / skip := true,
    unusedCompileDependenciesFilter -= moduleFilter("org.scala-js", "scalajs-library")
  )
  .aggregate(
    awsLambdaConnector,
    couchbaseConnector,
    dynamodbConnector,
    fileConnector,
    fs2Connector,
    kafkaConnector,
    s3Connector,
    docs
  )

lazy val awsLambdaConnector = project
  .in(file("connectors/aws-lambda-connector"))
  .settings(stdSettings("zio-connect-aws-lambda"))
  .settings(
    enableZIO(enableStreaming = true),
    libraryDependencies ++= Seq(
      AWSLambdaDependencies.`aws-java-sdk-core`,
      AWSLambdaDependencies.localstack,
      AWSLambdaDependencies.`zio-aws-lambda`,
      AWSLambdaDependencies.`zio-aws-netty`
    )
  )
  .settings(scalaCompatSettings)
  .settings(Test / fork := true)

lazy val couchbaseConnector = project
  .in(file("connectors/couchbase-connector"))
  .settings(stdSettings("zio-connect-couchbase"))
  .settings(
    libraryDependencies ++= Seq(
      CouchbaseDependencies.couchbase,
      CouchbaseDependencies.couchbaseContainer,
      CouchbaseDependencies.`zio-prelude`
    )
  )
  .settings(enableZIO(enableStreaming = true))
  .settings(scalaCompatSettings)
  .settings(Test / fork := true)

lazy val dynamodbConnector = project
  .in(file("connectors/dynamodb-connector"))
  .settings(stdSettings("zio-connect-dynamodb"))
  .settings(
    libraryDependencies ++= Seq(
      DynamoDBDependencies.`aws-java-sdk-core`,
      DynamoDBDependencies.`zio-aws-dynamodb`,
      DynamoDBDependencies.`zio-aws-netty`,
      DynamoDBDependencies.localstack
    )
  )
  .settings(enableZIO(enableStreaming = true))
  .settings(scalaCompatSettings)
  .settings(Test / fork := true)

lazy val fileConnector = project
  .in(file("connectors/file-connector"))
  .settings(stdSettings("zio-connect-file"))
  .settings(enableZIO(enableStreaming = true))
  .settings(scalaCompatSettings)

lazy val fs2Connector = project
  .in(file("connectors/fs2-connector"))
  .settings(stdSettings("zio-connect-fs2"))
  .settings(enableZIO(enableStreaming = true))
  .settings(
    libraryDependencies ++= Seq(
      FS2Dependencies.`fs2-core`,
      FS2Dependencies.`zio-interop-cats`
    )
  )
  .settings(addDependencyFor(scala211.value, scala212.value)(`scala-compact-collection`))
  .settings(Test / fork := true)

lazy val kafkaConnector = project
  .in(file("connectors/kafka-connector"))
  .settings(stdSettings("zio-connect-kafka"))
  .settings(enableZIO(enableStreaming = true))
  .settings(
    libraryDependencies ++= Seq(
      KafkaDependencies.`zio-kafka`,
      KafkaDependencies.`zio-kafka-test-utils`
    )
  )
  .settings(scalaCompatSettings)
  .settings(Test / fork := true)

lazy val kinesisDataStreamsConnector = project
  .in(file("connectors/kinesis-data-streams-connector"))
  .settings(stdSettings("zio-connect-kinesis-data-streams"))
  .settings(enableZIO(enableStreaming = true))
  .settings(
    libraryDependencies ++= Seq(
      KinesisDataStreamsDependencies.`aws-java-sdk-core`,
      KinesisDataStreamsDependencies.localstack,
      KinesisDataStreamsDependencies.`zio-aws-kinesis`
    )
  )
  .settings(scalaCompatSettings)
  .settings(Test / fork := true)

lazy val s3Connector = project
  .in(file("connectors/s3-connector"))
  .settings(stdSettings("zio-connect-s3"))
  .settings(enableZIO(enableStreaming = true))
  .settings(
    libraryDependencies ++= Seq(
      S3Dependencies.`aws-java-sdk-core`,
      S3Dependencies.localstack,
      S3Dependencies.`zio-aws-netty`,
      S3Dependencies.`zio-aws-s3`
    )
  )
  .settings(scalaCompatSettings)
  .settings(Test / fork := true)

/**
 * Examples Submodules
 */

lazy val examples = project
  .in(file("examples"))
  .settings(
    publishArtifact := false,
    moduleName      := "zio-connect-examples"
  )
  .aggregate(couchbaseConnectorExamples, dynamodbConnectorExamples, fileConnectorExamples, s3ConnectorExamples)

lazy val couchbaseConnectorExamples = project
  .in(file("examples/couchbase-connector-examples"))
  .settings(
    publish / skip := true,
    scalacOptions -= "-Xfatal-warnings",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio-json" % "0.3.0"
    )
  )
  .dependsOn(LocalProject("couchbaseConnector"))

lazy val dynamodbConnectorExamples = project
  .in(file("examples/dynamodb-connector-examples"))
  .settings(
    publish / skip := true,
    scalacOptions -= "-Xfatal-warnings",
    libraryDependencies := Seq(
      "dev.zio" %% "zio-aws-netty" % DynamoDBDependencies.zioAwsVersion
    )
  )
  .dependsOn(LocalProject("dynamodbConnector"))

lazy val fileConnectorExamples = project
  .in(file("examples/file-connector-examples"))
  .settings(
    publish / skip := true,
    scalacOptions -= "-Xfatal-warnings"
  )
  .dependsOn(LocalProject("fileConnector"))

lazy val s3ConnectorExamples = project
  .in(file("examples/s3-connector-examples"))
  .settings(
    publish / skip := true,
    scalacOptions -= "-Xfatal-warnings"
  )
  .dependsOn(LocalProject("s3Connector"))

lazy val docs = project
  .in(file("zio-connect-docs"))
  .settings(
    moduleName     := "zio-connect-docs",
    projectName    := (ThisBuild / name).value,
    mainModuleName := (fileConnector / moduleName).value,
    projectStage   := ProjectStage.ProductionReady,
    ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(
      awsLambdaConnector,
      couchbaseConnector,
      dynamodbConnector,
      fileConnector,
      fs2Connector,
      kafkaConnector,
      s3Connector
    )
  )
  .enablePlugins(WebsitePlugin)

lazy val scalaCompatSettings =
  addDependencyFor(scala211.value, scala212.value)(`scala-compact-collection`)

def addDependencyFor(scalaVersions: String*)(dependencies: ModuleID*) =
  libraryDependencies ++= {
    if (scalaVersions.contains(scalaVersion.value)) dependencies else Seq.empty
  }
