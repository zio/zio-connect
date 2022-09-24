addSbtPlugin("ch.epfl.scala"                     % "sbt-scalafix"              % "0.10.3")
addSbtPlugin("com.eed3si9n"                      % "sbt-buildinfo"             % "0.9.0")
addSbtPlugin("com.eed3si9n"                      % "sbt-unidoc"                % "0.4.3")
addSbtPlugin("com.geirsson"                      % "sbt-ci-release"            % "1.5.0")
addSbtPlugin("com.github.cb372"                  % "sbt-explicit-dependencies" % "0.2.12")
addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings"          % "3.0.0")
addSbtPlugin("org.portable-scala"                % "sbt-scalajs-crossproject"  % "1.0.0")
addSbtPlugin("org.scala-js"                      % "sbt-scalajs"               % "1.10.1")
addSbtPlugin("org.scalameta"                     % "sbt-mdoc"                  % "2.1.1")
addSbtPlugin("org.scalameta"                     % "sbt-scalafmt"              % "2.3.2")
addSbtPlugin("org.scoverage"                     % "sbt-scoverage"             % "1.6.1")
addSbtPlugin("pl.project13.scala"                % "sbt-jmh"                   % "0.3.7")

libraryDependencies += "org.snakeyaml" % "snakeyaml-engine" % "2.4"
