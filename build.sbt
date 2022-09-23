import BuildHelper._
import explicitdeps.ExplicitDepsPlugin.autoImport.moduleFilterRemoveValue
import sbtcrossproject.CrossPlugin.autoImport.crossProject

inThisBuild(
  List(
    organization := "dev.zio",
    homepage     := Some(url("https://zio.github.io/zio-connect/")),
    licenses     := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(
      Developer(
        "jdegoes",
        "John De Goes",
        "john@degoes.net",
        url("http://degoes.net")
      )
    ),
    pgpPassphrase := sys.env.get("PGP_PASSWORD").map(_.toArray),
    pgpPublicRing := file("/tmp/public.asc"),
    pgpSecretRing := file("/tmp/secret.asc"),
    scmInfo := Some(
      ScmInfo(url("https://github.com/zio/zio-connect/"), "scm:git:git@github.com:zio/zio-connect.git")
    )
  )
)

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

val zioVersion    = "2.0.2"
val zioNioVersion = "2.0.0"

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
  .settings(stdSettings("zioConnect"))
  .settings(crossProjectSettings)
  .settings(buildInfoSettings("zio.connect"))
  .settings(
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio"         % zioVersion,
      "dev.zio" %% "zio-streams" % zioVersion,
      "dev.zio" %% "zio-nio"     % zioNioVersion,
//      "dev.zio" %% "zio-s3"       % "0.2.1",
      "dev.zio" %% "zio-test"     % zioVersion % "test",
      "dev.zio" %% "zio-test-sbt" % zioVersion % "test"
    )
  )
  .settings(testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"))
  .enablePlugins(BuildInfoPlugin)

lazy val zioConnectJS = zioConnect.js
  .settings(scalaJSUseMainModuleInitializer := true)

lazy val zioConnectJVM = zioConnect.jvm
  .settings(dottySettings)

lazy val docs = project
  .in(file("zio-connect-docs"))
  .settings(
    publish / skip := true,
    moduleName     := "zio-connect-docs",
    scalacOptions -= "-Yno-imports",
    scalacOptions -= "-Xfatal-warnings",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % zioVersion
    ),
    ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(root),
    ScalaUnidoc / unidoc / target              := (LocalRootProject / baseDirectory).value / "website" / "static" / "api",
    cleanFiles += (ScalaUnidoc / unidoc / target).value,
    docusaurusCreateSite     := docusaurusCreateSite.dependsOn(Compile / unidoc).value,
    docusaurusPublishGhpages := docusaurusPublishGhpages.dependsOn(Compile / unidoc).value
  )
  .dependsOn(root)
  .enablePlugins(MdocPlugin, DocusaurusPlugin, ScalaUnidocPlugin)
