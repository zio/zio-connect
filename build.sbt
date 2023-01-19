import BuildHelper._
import Dependencies._
import explicitdeps.ExplicitDepsPlugin.autoImport.moduleFilterRemoveValue

inThisBuild(
  List(
    organization := "dev.zio",
    homepage     := Some(url("https://zio.dev/zio-connect")),
    licenses := List(
      "Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")
    ),
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
    s3Connector,
    docs
  )
  .enablePlugins(BuildInfoPlugin)

lazy val awsLambdaConnector = project
  .in(file("connectors/aws-lambda-connector"))
  .settings(stdSettings("zio-connect-aws-lambda"))
  .settings(
    libraryDependencies ++= Seq(
      AWSLambdaDependencies.`aws-java-sdk-core`,
      AWSLambdaDependencies.localstack,
      AWSLambdaDependencies.`zio-aws-lambda`,
      AWSLambdaDependencies.`zio-aws-netty`,
      zio,
      `zio-streams`,
      `zio-test`,
      `zio-test-sbt`
    )
  )
  .settings(
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, n)) if n <= 12 => Seq(`scala-compact-collection`)
        case _                       => Seq.empty
      }
    }
  )
  .settings(
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    Test / fork := true
  )

lazy val couchbaseConnector = project
  .in(file("connectors/couchbase-connector"))
  .settings(stdSettings("zio-connect-couchbase"))
  .settings(
    libraryDependencies ++= Seq(
      CouchbaseDependencies.couchbase,
      CouchbaseDependencies.couchbaseContainer,
      CouchbaseDependencies.`zio-prelude`,
      zio,
      `zio-streams`,
      `zio-test`,
      `zio-test-sbt`
    )
  )
  .settings(
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, n)) if n <= 12 => Seq(`scala-compact-collection`)
        case _                       => Seq.empty
      }
    }
  )
  .settings(
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    Test / fork := true
  )

lazy val dynamodbConnector = project
  .in(file("connectors/dynamodb-connector"))
  .settings(stdSettings("zio-connect-dynamodb"))
  .settings(
    libraryDependencies ++= Seq(
      DynamoDBDependencies.`aws-java-sdk-core`,
      DynamoDBDependencies.`zio-aws-dynamodb`,
      DynamoDBDependencies.`zio-aws-netty`,
      DynamoDBDependencies.localstack,
      zio,
      `zio-streams`,
      `zio-test`,
      `zio-test-sbt`
    )
  )
  .settings(
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, n)) if n <= 12 => Seq(`scala-compact-collection`)
        case _                       => Seq.empty
      }
    }
  )
  .settings(
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    Test / fork := true
  )

lazy val fileConnector = project
  .in(file("connectors/file-connector"))
  .settings(stdSettings("zio-connect-file"))
  .settings(
    libraryDependencies ++= Seq(
      zio,
      `zio-streams`,
      `zio-test`,
      `zio-test-sbt`
    )
  )
  .settings(
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, n)) if n <= 12 => Seq(`scala-compact-collection`)
        case _                       => Seq.empty
      }
    }
  )
  .settings(testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"))

lazy val fs2Connector = project
  .in(file("connectors/fs2-connector"))
  .settings(stdSettings("zio-connect-fs2"))
  .settings(
    libraryDependencies ++= Seq(
      FS2Dependencies.`fs2-core`,
      FS2Dependencies.`zio-interop-cats`,
      zio,
      `zio-streams`,
      `zio-test`,
      `zio-test-sbt`
    )
  )
  .settings(
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, n)) if n <= 12 => Seq(`scala-compact-collection`)
        case _                       => Seq.empty
      }
    }
  )
  .settings(
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    Test / fork := true
  )

lazy val kafkaConnector = project
  .in(file("connectors/kafka-connector"))
  .settings(stdSettings("zio-connect-kafka"))
  .settings(
    libraryDependencies ++= Seq(
      KafkaDependencies.`zio-kafka`,
      KafkaDependencies.`zio-kafka-test-utils`,
      zio,
      `zio-streams`,
      `zio-test`,
      `zio-test-sbt`
    )
  )
  .settings(
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, n)) if n <= 12 => Seq(`scala-compact-collection`)
        case _                       => Seq.empty
      }
    }
  )
  .settings(
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    Test / fork := true
  )

lazy val kinesisDataStreamsConnector = project
  .in(file("connectors/kinesis-data-streams-connector"))
  .settings(stdSettings("zio-connect-kinesis-data-streams"))
  .settings(
    libraryDependencies ++= Seq(
      KinesisDataStreamsDependencies.`aws-java-sdk-core`,
      KinesisDataStreamsDependencies.localstack,
      KinesisDataStreamsDependencies.`zio-aws-kinesis`,
      zio,
      `zio-streams`,
      `zio-test`,
      `zio-test-sbt`
    )
  )
  .settings(
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, n)) if n <= 12 => Seq(`scala-compact-collection`)
        case _                       => Seq.empty
      }
    }
  )
  .settings(
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    Test / fork := true
  )

lazy val s3Connector = project
  .in(file("connectors/s3-connector"))
  .settings(stdSettings("zio-connect-s3"))
  .settings(
    libraryDependencies ++= Seq(
      S3Dependencies.`aws-java-sdk-core`,
      S3Dependencies.localstack,
      S3Dependencies.`zio-aws-netty`,
      S3Dependencies.`zio-aws-s3`,
      zio,
      `zio-streams`,
      `zio-test`,
      `zio-test-sbt`
    )
  )
  .settings(
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, n)) if n <= 12 => Seq(`scala-compact-collection`)
        case _                       => Seq.empty
      }
    }
  )
  .settings(
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    Test / fork := true
  )

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
    moduleName        := "zio-connect-docs",
    projectName       := "ZIO Connect",
    mainModuleName    := (fileConnector / moduleName).value,
    projectStage      := ProjectStage.ProductionReady,
    docsPublishBranch := "master",
    ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(
      awsLambdaConnector,
      couchbaseConnector,
      dynamodbConnector,
      fileConnector,
      fs2Connector,
      s3Connector
    )
  )
  .enablePlugins(WebsitePlugin)
