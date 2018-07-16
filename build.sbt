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
    .settings(settings)
    .settings(
      libraryDependencies ++= Seq(
        library.akkaHttp,
        library.akkaHttpCirce,
        library.akkaStreamTyped,
        library.log4jApi,
        library.log4jApiScala,
        library.log4jCore,
        library.akkaActorTestkitTyped % Test,
        library.akkaHttpTestkit       % Test,
        library.scalaCheck            % Test,
        library.utest                 % Test
      )
    )

lazy val `streamee-demo` =
  project
    .enablePlugins(DockerPlugin, JavaAppPackaging)
    .dependsOn(`streamee`)
    .settings(settings)
    .settings(
      libraryDependencies ++= Seq(
        library.akkaClusterShardingTyped,
        library.akkaDiscoveryDns,
        library.akkaHttp,
        library.akkaHttpCirce,
        library.akkaLog4j,
        library.akkaManagementClusterBootstrap,
        library.akkaManagementClusterHttp,
        library.akkaStreamTyped,
        library.circeGeneric,
        library.disruptor,
        library.log4jApiScala,
        library.log4jCore,
        library.pureConfig
      )
    )

// *****************************************************************************
// Library dependencies
// *****************************************************************************

lazy val library =
  new {
    object Version {
      val akka           = "2.5.13"
      val akkaHttp       = "10.1.3"
      val akkaHttpJson   = "1.21.0"
      val akkaLog4j      = "1.6.1"
      val akkaManagement = "0.14.0"
      val circe          = "0.9.3"
      val disruptor      = "3.4.2"
      val log4j          = "2.11.0"
      val log4jApiScala  = "11.0"
      val pureConfig     = "0.9.1"
      val scalaCheck     = "1.14.0"
      val utest          = "0.6.4"
    }
    val akkaActorTestkitTyped          = "com.typesafe.akka"             %% "akka-actor-testkit-typed"          % Version.akka
    val akkaClusterShardingTyped       = "com.typesafe.akka"             %% "akka-cluster-sharding-typed"       % Version.akka
    val akkaDiscoveryDns               = "com.lightbend.akka.discovery"  %% "akka-discovery-dns"                % Version.akkaManagement
    val akkaHttp                       = "com.typesafe.akka"             %% "akka-http"                         % Version.akkaHttp
    val akkaHttpCirce                  = "de.heikoseeberger"             %% "akka-http-circe"                   % Version.akkaHttpJson
    val akkaHttpTestkit                = "com.typesafe.akka"             %% "akka-http-testkit"                 % Version.akkaHttp
    val akkaLog4j                      = "de.heikoseeberger"             %% "akka-log4j"                        % Version.akkaLog4j
    val akkaManagementClusterBootstrap = "com.lightbend.akka.management" %% "akka-management-cluster-bootstrap" % Version.akkaManagement
    val akkaManagementClusterHttp      = "com.lightbend.akka.management" %% "akka-management-cluster-http"      % Version.akkaManagement
    val akkaStreamTyped                = "com.typesafe.akka"             %% "akka-stream-typed"                 % Version.akka
    val circeGeneric                   = "io.circe"                      %% "circe-generic"                     % Version.circe
    val disruptor                      = "com.lmax"                      %  "disruptor"                         % Version.disruptor
    val log4jApi                       = "org.apache.logging.log4j"      %  "log4j-api"                         % Version.log4j
    val log4jApiScala                  = "org.apache.logging.log4j"      %% "log4j-api-scala"                   % Version.log4jApiScala
    val log4jCore                      = "org.apache.logging.log4j"      %  "log4j-core"                        % Version.log4j
    val pureConfig                     = "com.github.pureconfig"         %% "pureconfig"                        % Version.pureConfig
    val scalaCheck                     = "org.scalacheck"                %% "scalacheck"                        % Version.scalaCheck
    val utest                          = "com.lihaoyi"                   %% "utest"                             % Version.utest
  }

// *****************************************************************************
// Settings
// *****************************************************************************

lazy val settings =
  commonSettings ++
  scalafmtSettings ++
  dockerSettings ++
  commandAliases

lazy val commonSettings =
  Seq(
    scalaVersion := "2.12.6",
    organization := "io.moia",
    organizationName := "MOIA GmbH",
    startYear := Some(2018),
    scalacOptions ++= Seq(
      "-unchecked",
      "-deprecation",
      "-language:_",
      "-target:jvm-1.8",
      "-encoding", "UTF-8",
      "-Ypartial-unification",
      "-Ywarn-unused-import"
    ),
    Compile / unmanagedSourceDirectories := Seq((Compile / scalaSource).value),
    Test / unmanagedSourceDirectories := Seq((Test / scalaSource).value),
    Compile / packageDoc / publishArtifact := false,
    Compile / packageSrc / publishArtifact := false,
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
    dockerBaseImage := "openjdk:10.0.1-slim",
    dockerExposedPorts := Seq(80, 8558)
  )

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
