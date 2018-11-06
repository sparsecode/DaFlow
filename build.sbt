/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import sbt.Keys.libraryDependencies

name := "etl_framework"

lazy val etlFrameworkCommonSettings = Seq(
  scalacOptions += "-target:jvm-1.8",
  version := "0.1.0",
  organization := "com.abhioncbr.etlFramework",
  scalaVersion := "2.11.11"
)

resolvers ++= Seq(
  Resolver.sonatypeRepo(status = "releases"),
  Resolver.sonatypeRepo(status = "snapshots")
)

lazy val logback = "1.1.7"
lazy val scalaMock = "4.1.0"
lazy val scalaTest = "3.0.4"
lazy val scalaLogging = "3.9.0"
lazy val scoptVersion = "3.7.0"
lazy val sparkVersion = "2.3.1"
lazy val mysqlVersion = "5.1.25"
lazy val jodaTimeVersion = "2.9.4"
lazy val jcommanderVersion = "1.48"
lazy val prometheusVersion = "0.0.15"

libraryDependencies in ThisBuild ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",

  "com.github.scopt" %% "scopt" % scoptVersion,
  "joda-time" % "joda-time" % jodaTimeVersion,
//  "com.beust" % "jcommander" % jcommanderVersion,

  "io.prometheus" % "simpleclient" % prometheusVersion,
  "io.prometheus" % "simpleclient_servlet" % prometheusVersion,
  "io.prometheus" % "simpleclient_pushgateway" % prometheusVersion,

  "org.scalamock" % "scalamock_2.11" % scalaMock % Test,
  "org.scalatest" % "scalatest_2.11" % scalaTest % Test,

  "ch.qos.logback" % "logback-classic" % logback,
  "com.typesafe.scala-logging" %% "scala-logging" % scalaLogging,

  "mysql" % "mysql-connector-java" % mysqlVersion
)

lazy val etlCoreString = "etlCore"
lazy val etlMetricsString = "etlMetrics"
lazy val etlJobConfString = "etlJobConf"
lazy val etlCommonsString = "etlCommons"
lazy val etlSqlParserString = "etlSqlParser"

lazy val etlFramework = project.in(file("."))
  .dependsOn(deps = etlCoreString).aggregate(refs = etlCoreString)
  .dependsOn(deps = etlCommonsString).aggregate(refs = etlCommonsString)
  .dependsOn(deps = etlJobConfString).aggregate(refs = etlJobConfString)
  .dependsOn(deps = etlMetricsString).aggregate(refs = etlMetricsString)
  .dependsOn(deps = etlSqlParserString).aggregate(refs = etlSqlParserString)
  .settings(etlFrameworkCommonSettings: _*)

lazy val etlCore = project.in(file("etl_core"))
  .dependsOn(deps = etlCommonsString).aggregate(refs = etlCommonsString)
  .dependsOn(deps = etlJobConfString).aggregate(refs = etlJobConfString)
  .dependsOn(deps = etlMetricsString).aggregate(refs = etlMetricsString)
  .dependsOn(deps = etlSqlParserString).aggregate(refs = etlSqlParserString)
  .settings(etlFrameworkCommonSettings: _*)
  .settings(mainClass in assembly := Some("com.abhioncbr.etlFramework.core.LaunchETLExecution"))

lazy val etlSqlParser = project.in(file("etl_sql_parser"))
  .settings(etlFrameworkCommonSettings: _*)

lazy val etlCommons = project.in(file("etl_commons"))
  .dependsOn(deps = etlSqlParserString).aggregate(refs = etlSqlParserString)
  .settings(etlFrameworkCommonSettings: _*)

lazy val etlJobConf = project.in(file("etl_job_conf"))
  .dependsOn(etlCommons % "compile->compile;test->test").aggregate(refs = etlCommonsString)
  .settings(etlFrameworkCommonSettings: _*)

lazy val etlMetrics = project.in(file("etl_metrics"))
  .dependsOn(deps = etlCommonsString).aggregate(refs = etlCommonsString)
  .settings(etlFrameworkCommonSettings: _*)