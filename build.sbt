import BuildHelper._
import Dependencies._
import explicitdeps.ExplicitDepsPlugin.autoImport.moduleFilterRemoveValue

inThisBuild(
  List(
    organization := "dev.zio",
    homepage := Some(url("https://zio.dev")),
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
    fileConnector,
    docs
  )

lazy val fileConnector = project
  .in(file("connectors/file-connector"))
  .settings(stdSettings("zio-connect-file"))
  .settings(
    libraryDependencies ++= Seq(
      `zio`,
      `zio-streams`,
      `zio-test`,
      `zio-test-sbt`
    )
  )
  .settings(
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, n)) if n <= 12 => Seq(`scala-compact-collection`)
        case _ => Seq.empty
      }
    }
  )
  .settings(testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"))
  .enablePlugins(BuildInfoPlugin)

lazy val ftpConnector = project
  .in(file("connectors/ftp-connector"))
  .settings(stdSettings("zio-connect-ftp"))
  .settings(
    libraryDependencies ++= Seq(
      FtpDependencies.zioFtp,
      FtpDependencies.testContainers,
      FtpDependencies.testContainersJupiter,
      FtpDependencies.`junit-jupiter`,
      `zio`,
      `zio-streams`,
      `zio-prelude`,
      "dev.zio" %% "zio-aws-s3" % "5.17.280.1",
      `zio-test`,
      `zio-test-sbt`,
    )
  )
  .settings(
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, n)) if n <= 12 => Seq(`scala-compact-collection`)
        case _ => Seq.empty
      }
    }
  )
  .settings(testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"))
  .enablePlugins(BuildInfoPlugin)

lazy val docs = project
  .in(file("zio-connect-docs"))
  .settings(
    publish / skip := true,
    moduleName := "zio-connect-docs",
    scalacOptions -= "-Yno-imports",
    scalacOptions -= "-Xfatal-warnings",
    libraryDependencies ++= Seq(
      `zio`
    ),
    ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(fileConnector),
    ScalaUnidoc / unidoc / target := (LocalRootProject / baseDirectory).value / "website" / "static" / "api",
    cleanFiles += (ScalaUnidoc / unidoc / target).value,
    docusaurusCreateSite := docusaurusCreateSite.dependsOn(Compile / unidoc).value,
    docusaurusPublishGhpages := docusaurusPublishGhpages.dependsOn(Compile / unidoc).value
  )
  .dependsOn(fileConnector)
  .enablePlugins(MdocPlugin, DocusaurusPlugin, ScalaUnidocPlugin)

lazy val examples = project
  .in(file("examples"))
  .settings(
    publishArtifact := false,
    moduleName := "zio-connect-examples"
  )
  .aggregate(fileConnectorExamples)

lazy val fileConnectorExamples = project
  .in(file("examples/file-connector-examples"))
  .settings(
    publish / skip := true,
    scalacOptions -= "-Xfatal-warnings"
  )
  .dependsOn(LocalProject("fileConnector"))
