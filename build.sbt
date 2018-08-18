import sbt.Keys.libraryDependencies

name := "etl_framework"

lazy val etl_framework_common_settings = Seq(
  scalacOptions += "-target:jvm-1.7",
  version := "0.1.0",
  organization := "com.abhioncbr.etlFramework",
  scalaVersion := "2.11.11"
)

val versions = new {
  val scoptVersion = "3.5.0"
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
  "com.google.guava" % "guava" % "13.0",
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

lazy val etl_framework = project.in(file(".")).
  dependsOn("etl_feed").aggregate("etl_feed")
  .dependsOn("etl_sql_parser").aggregate("etl_sql_parser")
  .settings(etl_framework_common_settings: _*)

lazy val etl_feed = project.in(file("etl_feed"))
  .dependsOn("etl_sql_parser").aggregate("etl_sql_parser")
  .settings(etl_framework_common_settings: _*)
  .settings(mainClass in assembly := Some("com.lzd.etlFramework.etl.feed.LaunchETLExecution"))

lazy val etl_sql_parser = project.in(file("etl_sql_parser"))
  .settings(etl_framework_common_settings: _*)