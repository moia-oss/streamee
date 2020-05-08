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
      publishArtifact := false,
    )

lazy val `streamee` =
  project
    .enablePlugins(AutomateHeaderPlugin)
    .settings(settings)
    .settings(
      libraryDependencies ++= Seq(
        library.akkaHttp,
        library.akkaStreamTyped,
        library.slf4jApi,
        library.akkaActorTestkitTyped   % Test,
        library.akkaHttpTestkit         % Test,
        library.akkaStreamTestkit       % Test,
        library.log4jCore               % Test,
        library.log4jSlf4j              % Test,
        library.scalaCheck              % Test,
        library.scalaTest               % Test,
        library.scalaTestPlusScalaCheck % Test,
      )
    )

lazy val `streamee-demo` =
  project
    .enablePlugins(AutomateHeaderPlugin, JavaAppPackaging, DockerPlugin)
    .dependsOn(`streamee`)
    .settings(settings)
    .settings(
      libraryDependencies ++= Seq(
        library.akkaManagementClusterBootstrap,
        library.akkaClusterShardingTyped,
        library.akkaDiscovery,
        library.akkaDiscoveryK8s,
        library.akkaHttpCirce,
        library.akkaHttpSprayJson,
        library.akkaSlf4j,
        library.circeGeneric,
        library.disruptor,
        library.log4jCore,
        library.log4jSlf4j,
        library.pureConfig,
      ),
      publishArtifact := false,
      Docker / maintainer := organizationName.value,
      Docker / version := "latest",
      dockerBaseImage := "adoptopenjdk:8u232-b09-jdk-hotspot",
      dockerExposedPorts := Seq(8080, 8558, 25520),
    )

// *****************************************************************************
// Library dependencies
// *****************************************************************************

lazy val library =
  new {
    object Version {
      val akka                    = "2.6.5"
      val akkaManagement          = "1.0.7"
      val akkaHttp                = "10.1.11"
      val akkaHttpJson            = "1.32.0"
      val akkaLog4j               = "1.6.1"
      val circe                   = "0.13.0"
      val disruptor               = "3.4.2"
      val log4j                   = "2.13.2"
      val pureConfig              = "0.12.3"
      val scalaCheck              = "1.14.3"
      val scalaTest               = "3.1.1"
      val scalaTestPlusScalaCheck = "3.1.1.1"
      val slf4j                   = "1.7.30"
    }
    val akkaActorTestkitTyped          = "com.typesafe.akka"             %% "akka-actor-testkit-typed"          % Version.akka
    val akkaManagementClusterBootstrap = "com.lightbend.akka.management" %% "akka-management-cluster-bootstrap" % Version.akkaManagement
    val akkaClusterShardingTyped       = "com.typesafe.akka"             %% "akka-cluster-sharding-typed"       % Version.akka
    val akkaDiscovery                  = "com.typesafe.akka"             %% "akka-discovery"                    % Version.akka
    val akkaDiscoveryK8s               = "com.lightbend.akka.discovery"  %% "akka-discovery-kubernetes-api"     % Version.akkaManagement
    val akkaHttp                       = "com.typesafe.akka"             %% "akka-http"                         % Version.akkaHttp
    val akkaHttpCirce                  = "de.heikoseeberger"             %% "akka-http-circe"                   % Version.akkaHttpJson
    val akkaHttpSprayJson              = "com.typesafe.akka"             %% "akka-http-spray-json"              % Version.akkaHttp
    val akkaHttpTestkit                = "com.typesafe.akka"             %% "akka-http-testkit"                 % Version.akkaHttp
    val akkaSlf4j                      = "com.typesafe.akka"             %% "akka-slf4j"                        % Version.akka
    val akkaStreamTestkit              = "com.typesafe.akka"             %% "akka-stream-testkit"               % Version.akka
    val akkaStreamTyped                = "com.typesafe.akka"             %% "akka-stream-typed"                 % Version.akka
    val circeGeneric                   = "io.circe"                      %% "circe-generic"                     % Version.circe
    val disruptor                      = "com.lmax"                      %  "disruptor"                         % Version.disruptor
    val log4jCore                      = "org.apache.logging.log4j"      %  "log4j-core"                        % Version.log4j
    val log4jSlf4j                     = "org.apache.logging.log4j"      %  "log4j-slf4j-impl"                  % Version.log4j
    val pureConfig                     = "com.github.pureconfig"         %% "pureconfig"                        % Version.pureConfig
    val scalaCheck                     = "org.scalacheck"                %% "scalacheck"                        % Version.scalaCheck
    val scalaTest                      = "org.scalatest"                 %% "scalatest"                         % Version.scalaTest
    val scalaTestPlusScalaCheck        = "org.scalatestplus"             %% "scalacheck-1-14"                   % Version.scalaTestPlusScalaCheck
    val slf4jApi                       = "org.slf4j"                     %  "slf4j-api"                         % Version.slf4j
  }

// *****************************************************************************
// Settings
// *****************************************************************************

lazy val settings =
  commonSettings ++
  scalafmtSettings ++
  sonatypeSettings ++
  commandAliases

lazy val commonSettings =
  Seq(
    scalaVersion := "2.13.1",
    crossScalaVersions := Seq(scalaVersion.value, "2.12.10"),
    organization := "io.moia",
    organizationName := "MOIA GmbH",
    startYear := Some(2018),
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
    scalacOptions ++= Seq(
      "-unchecked",
      "-deprecation",
      "-language:_",
      "-target:jvm-1.8",
      "-encoding", "UTF-8",
    ),
    Compile / unmanagedSourceDirectories := Seq((Compile / scalaSource).value),
    Test / unmanagedSourceDirectories := Seq((Test / scalaSource).value),
  )

lazy val scalafmtSettings =
  Seq(
    scalafmtOnCompile := true,
  )

lazy val sonatypeSettings = {
  import xerial.sbt.Sonatype._
  Seq(
    publishTo := sonatypePublishToBundle.value,
    sonatypeProfileName := organization.value,
    publishMavenStyle := true,
    sonatypeProjectHosting := Some(GitHubHosting("moia-dev", "streamee", "support@moia.io")),
  )
}

lazy val commandAliases =
  addCommandAlias(
    "r1",
    """|reStart
       |---
       |-Dakka.cluster.seed-nodes.0=akka://streamee-demo@127.0.0.1:25520
       |-Dakka.management.cluster.bootstrap.contact-point-discovery.discovery-method=config
       |-Dakka.management.http.hostname=127.0.0.1
       |-Dakka.remote.artery.canonical.hostname=127.0.0.1
       |-Dstreamee-demo.api.interface=127.0.0.1
       |""".stripMargin
  ) ++
  addCommandAlias(
    "r2",
    """|reStart
       |---
       |-Dakka.cluster.seed-nodes.0=akka://streamee-demo@127.0.0.1:25520
       |-Dakka.management.cluster.bootstrap.contact-point-discovery.discovery-method=config
       |-Dakka.management.http.hostname=127.0.0.2
       |-Dakka.remote.artery.canonical.hostname=127.0.0.2
       |-Dstreamee-demo.api.interface=127.0.0.2
       |""".stripMargin
  )
