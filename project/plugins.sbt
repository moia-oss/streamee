addSbtPlugin("com.dwijnand"     % "sbt-dynver"          % "3.0.0")
addSbtPlugin("com.geirsson"     % "sbt-scalafmt"        % "1.5.1")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.3.6")
addSbtPlugin("io.spray"         % "sbt-revolver"        % "0.9.1")

libraryDependencies += "org.slf4j" % "slf4j-nop" % "1.7.25" // Needed by sbt-git
