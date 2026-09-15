ThisBuild / scalaVersion := "2.12.20"
ThisBuild / version := "0.3.0"
ThisBuild / organization := "edu.itmo"

lazy val sparkVersion = "3.5.9"
lazy val jacksonVersion = "2.15.2"
lazy val hbaseConnectorVersion = "1.0.1"
lazy val hbaseVersion = "2.6.6-hadoop3"

lazy val root = (project in file("."))
  .settings(
    name := "openfoodfacts-data-mart",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided,
      ("org.apache.hbase.connectors.spark" % "hbase-spark" % hbaseConnectorVersion)
        .excludeAll(
          ExclusionRule(organization = "org.apache.spark"),
          ExclusionRule(organization = "org.apache.hadoop"),
          ExclusionRule(organization = "org.scala-lang", name = "scala-library")
        ),
      ("org.slf4j" % "slf4j-nop" % "1.7.36")
        .exclude("org.slf4j", "slf4j-api"),
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
      "org.scalameta" %% "munit" % "1.1.1" % Test
    ),
    dependencyOverrides ++= Seq(
      "org.apache.hbase" % "hbase-shaded-client" % hbaseVersion,
      "org.apache.hbase" % "hbase-shaded-mapreduce" % hbaseVersion
    ),
    Test / fork := true,
    Test / javaOptions ++= Seq(
      "-Dspark.ui.enabled=false",
      "-Duser.timezone=UTC",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    ),
    assembly / mainClass := Some("edu.itmo.datamart.Main"),
    assembly / assemblyJarName := "openfoodfacts-data-mart.jar",
    assembly / test := {},
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "module-info.class"          => MergeStrategy.discard
      case other                        => MergeStrategy.first
    }
  )