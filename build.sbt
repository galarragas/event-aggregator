import sbt.Keys.resolvers
import sbt.Resolver
import sbtdocker.DockerPlugin.autoImport._
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
import sbtrelease.ReleasePlugin.autoImport._
import sbtrelease.Version.Bump.Next
import sbtrelease.{Version, versionFormatError}

net.virtualvoid.sbt.graph.Plugin.graphSettings

name := "event-aggregator"

version := "1.0"

scalaVersion := "2.11.6"

lazy val LogbackVersion = "1.1.2"
lazy val AkkaVersion = "2.4.14"
lazy val AkkaHttpVersion = "10.0.0"
val AkkaStreamVersion = AkkaVersion
val AkkaStreamKafkaVersion = "0.13"
val Json4sVersion = "3.3.0"
val SchemaRegistryVersion = "3.1.0"
val ElasticsearchVersion = "2.3.4"
val Elastic4sVersion = "2.3.2"

enablePlugins(DockerPlugin)

val scopt = Seq(
  "com.github.scopt" %% "scopt" % "3.2.0"
)

val kafka = Seq(
  ("org.apache.kafka" %% "kafka" % "0.10.0.0")
    .excludeAll(
      ExclusionRule( organization = "log4j"),
      ExclusionRule( organization = "org.slf4j")
    )
)

val embeddedKafka = Seq("net.manub" %% "scalatest-embedded-kafka" % "0.10.0" % "it")

val elastic4s = Seq(
  "com.sksamuel.elastic4s" %% "elastic4s-core" % Elastic4sVersion,
  "com.sksamuel.elastic4s" %% "elastic4s-streams" % Elastic4sVersion
)


val elasticClients = Seq(
  "org.elasticsearch" % "elasticsearch" % ElasticsearchVersion
)


val jest = Seq( "io.searchbox" % "jest" % "2.0.0" )

val avro = Seq("org.apache.avro" % "avro" % "1.7.7")

val testCore = Seq(
  "org.scalatest" %% "scalatest" % "3.0.0" % "test, it",
  "org.mockito" % "mockito-all" % "1.9.5" % "test, it",
  "com.fasterxml.jackson.jaxrs" % "jackson-jaxrs-json-provider" % "2.3.2" % "it"
)

// Couldn't use PlayJson because of a conflict with the Jackson version used by Avro
val jsonParser = Seq(
  "org.json4s" %% "json4s-native" % "3.3.0" % "test"
)

val logback = Seq(
  "ch.qos.logback" % "logback-classic" % LogbackVersion
)

val apacheCommonsIo = Seq("commons-io" % "commons-io" % "2.0.1")

val kafkaStreams = Seq(
  ("com.typesafe.akka" %% "akka-stream-kafka" % AkkaStreamKafkaVersion).excludeAll(
    ExclusionRule( organization = "org.slf4j"),
    ExclusionRule( organization = "log4j")
  ),
  "com.typesafe.akka" %% "akka-stream-testkit" % AkkaStreamVersion % "test"
)

val akkaHttp = Seq(
  "com.typesafe.akka" %% "akka-http-core" % AkkaHttpVersion,
  "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-jackson" % AkkaHttpVersion
)

val json4s = Seq(
  "org.json4s" %% "json4s-jackson" % Json4sVersion,
  "org.json4s" %% "json4s-ext" % Json4sVersion
).map {
  _.excludeAll(
    ExclusionRule( organization = "joda-time"),
    ExclusionRule( organization = "org.joda")
  )
}


val akkaHttpJson = Seq("de.heikoseeberger" %% "akka-http-json4s" % "1.11.0")

val logging = Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
  "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion,
  "com.typesafe.akka" %% "akka-contrib" % AkkaVersion,
  "org.slf4j" % "log4j-over-slf4j" % "1.7.7"       //Kafka uses log4j explicilty. Look class kafka.utils.Logging
)

val confluentKafkaLibs = Seq(
  "io.confluent" % "kafka-schema-registry-client" % SchemaRegistryVersion,
  "io.confluent" % "kafka-avro-serializer" % SchemaRegistryVersion
).map(
  _.excludeAll(
    ExclusionRule( organization = "org.slf4j"),
    ExclusionRule( organization = "log4j")
  )
)

mainClass in assembly := Some("com.pragmasoft.eventaggregator.EventAggregatorApp")

test in assembly := {}
parallelExecution in IntegrationTest := false

val httpMetaEndpointLibraries = akkaHttp ++ json4s

libraryDependencies ++= scopt ++ testCore ++ logback ++ logging ++ avro ++ kafka ++ elastic4s ++
  jsonParser ++ kafkaStreams ++ confluentKafkaLibs ++ httpMetaEndpointLibraries ++
  jest ++ embeddedKafka ++ akkaHttpJson ++ apacheCommonsIo.map( _ % "it")

dependencyOverrides ++= Set(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"
)

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

resolvers ++= Seq (
  "Sonatype" at "https://oss.sonatype.org/content/groups/public/",
  "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/",
  "ConJars" at "http://conjars.org/repo",
  Resolver.bintrayRepo("hseeberger", "maven"),
  "Confluent" at "http://packages.confluent.io/maven/"
)

assemblyJarName in assembly := s"${name.value}.jar"

assemblyMergeStrategy in assembly := {
  case PathList("javax", "annotation", xs @ _*) => MergeStrategy.first
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
  case "application.conf" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

parallelExecution in IntegrationTest := false

Defaults.itSettings

lazy val eventAggregator = project.in(file("."))
  .configs(IntegrationTest)
  .settings(
    releaseVersionBump := Next,
    releaseTagName := s"${(version in ThisBuild).value}",
    releaseNextVersion := { ver => Version(ver).map(_.bump.string).getOrElse(versionFormatError) },
    releaseProcess := Seq[ReleaseStep](
      inquireVersions,
      tagRelease,
      setNextVersion,
      commitNextVersion,
      pushChanges
    )
  )

lazy val versionPropertiesTask = taskKey[Seq[File]]("Creating version.properties")

versionPropertiesTask := {
  val log = streams.value.log
  log.info("*** Generating version.properties")
  val f = (resourceManaged in Compile).value / "version.properties"
  IO.write(f, s"version=${version.value}")
  log.info(s"*** Generated version.properties")
  Seq(f)
}

resourceGenerators in Compile += versionPropertiesTask.taskValue

resourceGenerators in (Compile, assembly) += versionPropertiesTask.taskValue

// Make the docker task depend on the assembly task, which generates a fat JAR file
docker <<= (docker dependsOn assembly)

dockerfile in docker := {
  val artifact = (assemblyOutputPath in assembly).value
  val artifactTargetPath = s"/app/${artifact.name}"

  val configFile = baseDirectory.value / "config" / "application.conf"
  val configTargetPath = "/app/config/application.conf"

  val startScript =
    s"""|#!/bin/bash
        |
        |exec java $$JAVA_OPTS -Dconfig.file=$configTargetPath -jar $artifactTargetPath
    """.stripMargin

  val startScriptName = "start.sh"
  val startScriptFile = (target in compile).value / startScriptName
  val startScriptTargetPath = s"/app/$startScriptName"

  IO.writeLines(startScriptFile, Seq(startScript))

  new Dockerfile {
    from("open-jdk-8-jre")
    workDir("/app")
    add(configFile, configTargetPath)
    add(artifact, artifactTargetPath)
    add(startScriptFile,startScriptTargetPath)
    run("chmod", "a+x", startScriptTargetPath)
    entryPoint(
      startScriptTargetPath
    )
  }
}

imageNames in docker := Seq(
  ImageName(
    repository = name.value,
    registry = Some(organization.value),
    tag = Some(version.value)
  ),
  ImageName(
    repository = name.value,
    registry = Some(organization.value),
    tag = Some("latest")
  )
)

