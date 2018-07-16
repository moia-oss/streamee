addSbtPlugin("com.dwijnand"     % "sbt-dynver"          % "2.0.0")
addSbtPlugin("com.geirsson"     % "sbt-scalafmt"        % "1.5.0")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.3.3")
addSbtPlugin("io.spray"         % "sbt-revolver"        % "0.9.0")

libraryDependencies += "org.slf4j" % "slf4j-nop" % "1.7.25" // Needed by sbt-git
