import sbt.Keys.libraryDependencies

name := "etl_framework"

lazy val etlFrameworkCommonSettings = Seq(
  scalacOptions += "-target:jvm-1.8",
  version := "0.1.0",
  organization := "com.abhioncbr.etlFramework",
  scalaVersion := "2.11.11"
)

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)

val versions = new {
  val scoptVersion = "3.7.0"
  val sparkVersion = "2.3.1"
  val jodaTimeVersion = "2.9.4"
  val jcommanderVersion = "1.48"
  val prometheusVersion = "0.0.15"
}

libraryDependencies in ThisBuild ++= Seq(
  "org.apache.spark" %% "spark-core" % versions.sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % versions.sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % versions.sparkVersion % "provided",

  "com.github.scopt" %% "scopt" % versions.scoptVersion,
  "joda-time" % "joda-time" % versions.jodaTimeVersion,
  "com.beust" % "jcommander" % versions.jcommanderVersion,

  "io.prometheus" % "simpleclient" % versions.prometheusVersion,
  "io.prometheus" % "simpleclient_servlet" % versions.prometheusVersion,
  "io.prometheus" % "simpleclient_pushgateway" % versions.prometheusVersion,

  "org.scalamock" % "scalamock_2.11" % "4.1.0" % Test,
  "org.scalatest" % "scalatest_2.11" % "3.0.4" % Test,

  "ch.qos.logback" % "logback-classic" % "1.1.7",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",

  "mysql" % "mysql-connector-java" % "5.1.25"
)

lazy val etlFramework = project.in(file("."))
  .dependsOn(deps = "etlCore").aggregate("etlCore")
  .dependsOn(deps = "etlCommons").aggregate("etlCommons")
  .dependsOn(deps = "etlJobConf").aggregate("etlJobConf")
  .dependsOn(deps = "etlMetrics").aggregate("etlMetrics")
  .dependsOn(deps = "etlSqlParser").aggregate("etlSqlParser")
  .settings(etlFrameworkCommonSettings: _*)

lazy val etlCore = project.in(file("etl_core"))
  .dependsOn(deps = "etlCommons").aggregate("etlCommons")
  .dependsOn(deps = "etlSqlParser").aggregate("etlSqlParser")
  .dependsOn(deps = "etlJobConf").aggregate("etlJobConf")
  .dependsOn(deps = "etlMetrics").aggregate("etlMetrics")
  .settings(etlFrameworkCommonSettings: _*)
  .settings(mainClass in assembly := Some("com.abhioncbr.etlFramework.core.LaunchETLExecution"))

lazy val etlSqlParser = project.in(file("etl_sql_parser"))
  .settings(etlFrameworkCommonSettings: _*)

lazy val etlCommons = project.in(file("etl_commons"))
  .dependsOn(deps = "etlSqlParser").aggregate("etlSqlParser")
  .settings(etlFrameworkCommonSettings: _*)

lazy val etlJobConf = project.in(file("etl_job_conf"))
  .dependsOn(etlCommons % "compile->compile;test->test").aggregate("etlCommons")
  .settings(etlFrameworkCommonSettings: _*)

lazy val etlMetrics = project.in(file("etl_metrics"))
  .dependsOn(deps = "etlCommons").aggregate("etlCommons")
  .settings(etlFrameworkCommonSettings: _*)