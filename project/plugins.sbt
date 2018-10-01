addSbtPlugin("com.dwijnand"      % "sbt-dynver"          % "3.1.0")
addSbtPlugin("com.geirsson"      % "sbt-scalafmt"        % "1.5.1")
addSbtPlugin("com.typesafe.sbt"  % "sbt-native-packager" % "1.3.6")
addSbtPlugin("de.heikoseeberger" % "sbt-header"          % "5.0.0")
addSbtPlugin("io.spray"          % "sbt-revolver"        % "0.9.1")
addSbtPlugin("org.xerial.sbt"    % "sbt-sonatype"        % "2.3")
addSbtPlugin("com.jsuereth"      % "sbt-pgp"             % "1.1.1")

libraryDependencies += "org.slf4j" % "slf4j-nop" % "1.7.25" // Needed by sbt-git
