val zioSbtVersion = "0.3.10+74-a885a0a2-SNAPSHOT"

addSbtPlugin("dev.zio"      % "zio-sbt-ecosystem" % zioSbtVersion)
addSbtPlugin("dev.zio"      % "zio-sbt-website"   % zioSbtVersion)
addSbtPlugin("dev.zio"      % "zio-sbt-ci"        % zioSbtVersion)

addSbtPlugin("org.scoverage"                     % "sbt-scoverage"             % "1.6.1")

libraryDependencies += "org.snakeyaml" % "snakeyaml-engine" % "2.4"

resolvers ++= Resolver.sonatypeOssRepos("public")
