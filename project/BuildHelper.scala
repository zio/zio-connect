import Dependencies.silencerVersion
import com.jsuereth.sbtpgp.SbtPgp.autoImport.{pgpPassphrase, pgpPublicRing, pgpSecretRing}
import sbt.Keys._
import sbt._
import scalafix.sbt.ScalafixPlugin.autoImport._
import xerial.sbt.Sonatype.autoImport._

object BuildHelper extends ScalaSettings {
  val Scala212         = "2.12.16"
  val Scala213         = "2.13.9"
  val Scala3           = "3.2.0"
  val ScoverageVersion = "1.9.3"
  val JmhVersion       = "0.4.3"

  private val stdOptions = Seq(
    "-deprecation",
    "-encoding",
    "UTF-8",
    "-feature",
    "-unchecked",
    "-language:postfixOps"
  ) ++ {
    if (sys.env.contains("CI")) {
      Seq("-Xfatal-warnings")
    } else {
      Nil // to enable Scalafix locally
    }
  }

  def extraOptions(scalaVersion: String) =
    CrossVersion.partialVersion(scalaVersion) match {
      case Some((3, 0))  => scala3Settings
      case Some((2, 12)) => scala212Settings
      case Some((2, 13)) => scala213Settings
      case _             => Seq.empty
    }

  def publishSetting(publishArtifacts: Boolean) = {
    val publishSettings = Seq(
      organization     := "dev.zio",
      organizationName := "zio",
      licenses += ("MIT License", new URL("https://github.com/zio/zio-connect/blob/master/LICENSE")),
      sonatypeCredentialHost := "s01.oss.sonatype.org",
      sonatypeRepository     := "https://s01.oss.sonatype.org/service/local",
      sonatypeProfileName    := "dev.zio"
    )
    val skipSettings = Seq(
      publish / skip  := true,
      publishArtifact := false
    )
    if (publishArtifacts) publishSettings else publishSettings ++ skipSettings
  }

  def stdSettings(prjName: String) = Seq(
    name                           := s"$prjName",
    ThisBuild / crossScalaVersions := Seq(Scala212, Scala213, Scala3),
    ThisBuild / scalaVersion       := Scala213,
    scalacOptions                  := stdOptions ++ extraOptions(scalaVersion.value),
    semanticdbEnabled              := scalaVersion.value != Scala3, // enable SemanticDB
    semanticdbOptions += "-P:semanticdb:synthetics:on",
    semanticdbVersion                      := scalafixSemanticdb.revision, // use Scalafix compatible version
    ThisBuild / scalafixScalaBinaryVersion := CrossVersion.binaryScalaVersion(scalaVersion.value),
    ThisBuild / scalafixDependencies ++=
      List(
        "com.github.liancheng" %% "organize-imports" % "0.5.0",
        "com.github.vovapolu"  %% "scaluzzi"         % "0.1.23"
      ),
    Test / parallelExecution := true,
    incOptions ~= (_.withLogRecompileOnMacro(false)),
    autoAPIMappings  := true,
    ThisBuild / fork := true,
    libraryDependencies ++= {
      if (scalaVersion.value == Scala3)
        Seq(
          "com.github.ghik" % s"silencer-lib_$Scala213" % silencerVersion % Provided
        )
      else
        Seq(
          "com.github.ghik" % "silencer-lib" % silencerVersion % Provided cross CrossVersion.full,
          compilerPlugin("com.github.ghik" % "silencer-plugin" % silencerVersion cross CrossVersion.full)
        )
    }
  )

  def meta = Seq(
    ThisBuild / homepage := Some(url("https://github.com/zio/zio-connect")),
    ThisBuild / scmInfo :=
      Some(
        ScmInfo(url("https://github.com/zio/zio-connect"), "scm:git@github.com:zio/zio-connect.git")
      ),
    ThisBuild / homepage := Some(url("https://zio.github.io/zio-connect/")),
    ThisBuild / licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    ThisBuild / developers := List(
      Developer(
        "jdegoes",
        "John De Goes",
        "john@degoes.net",
        url("http://degoes.net")
      )
    ),
    ThisBuild / organization  := "dev.zio",
    ThisBuild / pgpPassphrase := sys.env.get("PGP_PASSWORD").map(_.toArray),
    ThisBuild / pgpPublicRing := file("/tmp/public.asc"),
    ThisBuild / pgpSecretRing := file("/tmp/secret.asc")
  )
}
