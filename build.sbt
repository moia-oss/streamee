// *****************************************************************************
// Projects
// *****************************************************************************

lazy val `streamee-root` =
  project
    .in(file("."))
    .aggregate(`streamee`, `streamee-demo`)
    .settings(settings)
    .settings(
      Compile / unmanagedSourceDirectories := Seq.empty,
      Test / unmanagedSourceDirectories := Seq.empty,
      publishArtifact := false
    )

lazy val `streamee` =
  project
    .enablePlugins(AutomateHeaderPlugin)
    .settings(settings)
    .settings(
      libraryDependencies ++= Seq(
        library.akkaHttp,
        library.akkaStreamTyped,
        library.log4jApi,
        library.slf4jApi,
        library.akkaActorTestkitTyped % Test,
        library.akkaHttpTestkit       % Test,
        library.akkaStreamTestkit     % Test,
        library.log4jCore             % Test,
        library.scalaCheck            % Test,
        library.utest                 % Test
      )
    )

lazy val `streamee-demo` =
  project
    .enablePlugins(AutomateHeaderPlugin, DockerPlugin, JavaAppPackaging)
    .dependsOn(`streamee`)
    .settings(settings)
    .settings(
      Compile / packageDoc / publishArtifact := false,
      Compile / packageSrc / publishArtifact := false,
      libraryDependencies ++= Seq(
        library.akkaClusterShardingTyped,
        library.akkaHttpCirce,
        library.akkaManagementClusterBootstrap,
        library.akkaManagementClusterHttp,
        library.akkaSlf4j,
        library.circeGeneric,
        library.disruptor,
        library.log4jCore,
        library.log4jSlf4j,
        library.pureConfig,
        library.slf4jApi
      ),
      publishArtifact := false
    )

// *****************************************************************************
// Library dependencies
// *****************************************************************************

lazy val library =
  new {
    object Version {
      val akka           = "2.6.5"
      val akkaHttp       = "10.1.11"
      val akkaHttpJson   = "1.32.0"
      val akkaManagement = "1.0.6"
      val circe          = "0.13.0"
      val disruptor      = "3.4.2"
      val log4j          = "2.13.2"
      val pureConfig     = "0.12.3"
      val scalaCheck     = "1.14.3"
      val utest          = "0.7.4"
      val slf4j          = "1.7.30"
    }
    val akkaActorTestkitTyped          = "com.typesafe.akka"             %% "akka-actor-testkit-typed"          % Version.akka
    val akkaClusterShardingTyped       = "com.typesafe.akka"             %% "akka-cluster-sharding-typed"       % Version.akka
    val akkaHttp                       = "com.typesafe.akka"             %% "akka-http"                         % Version.akkaHttp
    val akkaHttpCirce                  = "de.heikoseeberger"             %% "akka-http-circe"                   % Version.akkaHttpJson
    val akkaHttpTestkit                = "com.typesafe.akka"             %% "akka-http-testkit"                 % Version.akkaHttp
    val akkaManagementClusterBootstrap = "com.lightbend.akka.management" %% "akka-management-cluster-bootstrap" % Version.akkaManagement
    val akkaManagementClusterHttp      = "com.lightbend.akka.management" %% "akka-management-cluster-http"      % Version.akkaManagement
    val akkaSlf4j                      = "com.typesafe.akka"             %% "akka-slf4j"                        % Version.akka
    val akkaStreamTestkit              = "com.typesafe.akka"             %% "akka-stream-testkit"               % Version.akka
    val akkaStreamTyped                = "com.typesafe.akka"             %% "akka-stream-typed"                 % Version.akka
    val circeGeneric                   = "io.circe"                      %% "circe-generic"                     % Version.circe
    val disruptor                      = "com.lmax"                      %  "disruptor"                         % Version.disruptor
    val log4jApi                       = "org.apache.logging.log4j"      %  "log4j-api"                         % Version.log4j
    val log4jCore                      = "org.apache.logging.log4j"      %  "log4j-core"                        % Version.log4j
    val log4jSlf4j                     = "org.apache.logging.log4j"      %  "log4j-slf4j-impl"                  % Version.log4j
    val pureConfig                     = "com.github.pureconfig"         %% "pureconfig"                        % Version.pureConfig
    val scalaCheck                     = "org.scalacheck"                %% "scalacheck"                        % Version.scalaCheck
    val utest                          = "com.lihaoyi"                   %% "utest"                             % Version.utest
    val slf4jApi                       = "org.slf4j"                     %  "slf4j-api"                         % Version.slf4j
  }

// *****************************************************************************
// Settings
// *****************************************************************************

lazy val settings =
  commonSettings ++
  scalafmtSettings ++
  dockerSettings ++
  sonatypeSettings ++
  commandAliases

lazy val commonSettings =
  Seq(
    scalaVersion := "2.13.2",
    crossScalaVersions := Seq("2.12.10", "2.13.1"),
    organization := "io.moia",
    organizationName := "MOIA GmbH",
    startYear := Some(2018),
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
    scalacOptions ++= Seq(
      "-unchecked",
      "-deprecation",
      "-language:_",
      "-target:jvm-1.8",
      "-encoding", "UTF-8"
    ),
    Compile / unmanagedSourceDirectories := Seq((Compile / scalaSource).value),
    Test / unmanagedSourceDirectories := Seq((Test / scalaSource).value),
    testFrameworks += new TestFramework("utest.runner.Framework")
  )

lazy val scalafmtSettings =
  Seq(
    scalafmtOnCompile := true
  )

lazy val dockerSettings =
  Seq(
    Docker / daemonUser := "root",
    Docker / maintainer := organizationName.value,
    Docker / version := "latest",
    dockerBaseImage := "openjdk:10.0.2-slim",
    dockerExposedPorts := Seq(80, 8558)
  )

lazy val sonatypeSettings = {
  import xerial.sbt.Sonatype._
  Seq(
    publishTo := sonatypePublishTo.value,
    sonatypeProfileName := organization.value,
    publishMavenStyle := true,
    sonatypeProjectHosting := Some(GitHubHosting("moia-dev", "streamee", "support@moia.io"))
  )
}


lazy val commandAliases =
  addCommandAlias(
    "r0",
    """|reStart
       |---
       |-Dstreamee-demo.api.port=8080
       |-Dakka.management.http.hostname=127.0.0.1
       |-Dakka.management.http.port=8558
       |-Dakka.remote.artery.canonical.hostname=127.0.0.1
       |-Dakka.remote.artery.canonical.port=25520
       |-Dakka.cluster.seed-nodes.0=akka://streamee-demo@127.0.0.1:25520""".stripMargin
  ) ++
  addCommandAlias(
    "r1",
    """|reStart
       |---
       |-Dstreamee-demo.api.port=8081
       |-Dakka.management.http.hostname=127.0.0.1
       |-Dakka.management.http.port=8559
       |-Dakka.remote.artery.canonical.hostname=127.0.0.1
       |-Dakka.remote.artery.canonical.port=25521
       |-Dakka.cluster.seed-nodes.0=akka://streamee-demo@127.0.0.1:25520""".stripMargin
  )
