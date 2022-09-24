import BuildHelper._
import Dependencies._
import explicitdeps.ExplicitDepsPlugin.autoImport.moduleFilterRemoveValue
import sbtcrossproject.CrossPlugin.autoImport.crossProject

addCommandAlias("fmt", "all scalafmtSbt scalafmt Test/scalafmt")
addCommandAlias("fix", "; all Compile/scalafix Test/scalafix; all scalafmtSbt scalafmtAll")
addCommandAlias("check", "; scalafmtSbtCheck; scalafmtCheckAll; Compile/scalafix --check; Test/scalafix --check")

addCommandAlias(
  "testJVM",
  ";zioConnectJVM/test"
)
addCommandAlias(
  "testJS",
  ";zioConnectJS/test"
)

lazy val root = project
  .in(file("."))
  .settings(
    publish / skip := true,
    unusedCompileDependenciesFilter -= moduleFilter("org.scala-js", "scalajs-library")
  )
  .aggregate(
    zioConnectJVM,
    zioConnectJS
  )

lazy val zioConnect = crossProject(JSPlatform, JVMPlatform)
  .in(file("zio-connect"))
  .settings(stdSettings("zio-connect"))
  .settings(meta)
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
        case _                       => Seq.empty
      }
    }
  )
  .settings(testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"))
  .enablePlugins(BuildInfoPlugin)

lazy val zioConnectJS = zioConnect.js
  .settings(
    scalaJSUseMainModuleInitializer := true,
    Test / fork                     := false
  )

lazy val zioConnectJVM = zioConnect.jvm

lazy val docs = project
  .in(file("zio-connect-docs"))
  .settings(
    publish / skip := true,
    moduleName     := "zio-connect-docs",
    scalacOptions -= "-Yno-imports",
    scalacOptions -= "-Xfatal-warnings",
    libraryDependencies ++= Seq(
      `zio`
    ),
    ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(root),
    ScalaUnidoc / unidoc / target              := (LocalRootProject / baseDirectory).value / "website" / "static" / "api",
    cleanFiles += (ScalaUnidoc / unidoc / target).value,
    docusaurusCreateSite     := docusaurusCreateSite.dependsOn(Compile / unidoc).value,
    docusaurusPublishGhpages := docusaurusPublishGhpages.dependsOn(Compile / unidoc).value
  )
  .dependsOn(root)
  .enablePlugins(MdocPlugin, DocusaurusPlugin, ScalaUnidocPlugin)
