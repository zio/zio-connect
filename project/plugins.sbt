addSbtPlugin("ch.epfl.scala"                     % "sbt-scalafix"              % "0.10.3")
addSbtPlugin("com.eed3si9n"                      % "sbt-buildinfo"             % "0.11.0")
addSbtPlugin("com.github.sbt"                    % "sbt-unidoc"                % "0.5.0")
addSbtPlugin("com.github.sbt"                    % "sbt-ci-release"            % "1.5.10")
addSbtPlugin("com.github.cb372"                  % "sbt-explicit-dependencies" % "0.2.16")
addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings"          % "3.0.2")
addSbtPlugin("org.portable-scala"                % "sbt-scalajs-crossproject"  % "1.2.0")
addSbtPlugin("org.scala-js"                      % "sbt-scalajs"               % "1.10.1")
addSbtPlugin("org.scalameta"                     % "sbt-mdoc"                  % "2.3.2")
addSbtPlugin("org.scalameta"                     % "sbt-scalafmt"              % "2.4.6")
addSbtPlugin("org.scoverage"                     % "sbt-scoverage"             % "1.6.1")
addSbtPlugin("pl.project13.scala"                % "sbt-jmh"                   % "0.4.3")
addSbtPlugin("dev.zio"                           % "zio-sbt-website"           % "0.1.0+4-6fa03220-SNAPSHOT")

libraryDependencies += "org.snakeyaml" % "snakeyaml-engine" % "2.4"

resolvers += Resolver.sonatypeRepo("public")
