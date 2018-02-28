name := "etl_framework"

resolvers in ThisBuild ++= Seq(
  Resolver.sonatypeRepo("releases"),
  "Twitter Maven" at "https://maven.twttr.com",
  "Mapr Maven Repo" at "http://repository.mapr.com/maven/"
)

lazy val etl_framework_common_settings = Seq(
  scalacOptions += "-target:jvm-1.7",
  version := "0.1.0",
  organization := "com.lzd.etlFramework",
  scalaVersion := "2.10.6"
/*  assemblyMergeStrategy in assembly := {
    case "BUILD" => MergeStrategy.discard
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
    case PathList("javax", "xml", xs @ _*) => MergeStrategy.last
    case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
    case PathList("com", "google", xs @ _*) => MergeStrategy.last
    case PathList("org", "apache", xs @ _*) => MergeStrategy.last
    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
    case n if n.endsWith("-default.xml") => MergeStrategy.discard
    case "plugin.xml" => MergeStrategy.last
    case "parquet.thrift" => MergeStrategy.last
    case "overview.html" => MergeStrategy.rename
    case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
    case "META-INF/mailcap" => MergeStrategy.last
    case "META-INF/mimetypes.default" => MergeStrategy.last
    case other => MergeStrategy.defaultMergeStrategy(other)
  }*/
)

val versions = new {
  val hadoopVersion = "2.7.0"
  val scoptVersion = "3.5.0"
  val sparkVersion = "1.6.3"
  val jodaTimeVersion = "2.9.4"
  val jcommanderVersion = "1.48"
  val prometheusVersion = "0.0.15"
}

libraryDependencies in ThisBuild ++= Seq(
  //"org.apache.hadoop" % "hadoop-client" % versions.hadoopVersion,
  //"org.apache.spark" %% "spark-core" % versions.sparkVersion exclude("commons-collections", "commons-collections") exclude("commons-logging", "commons-logging") excludeAll ExclusionRule(organization = "org.apache.hadoop"),
  //"org.apache.spark" %% "spark-sql" % versions.sparkVersion  exclude("commons-collections", "commons-collections") exclude("commons-logging", "commons-logging") excludeAll ExclusionRule(organization = "org.apache.hadoop"),
  //"org.apache.spark" %% "spark-hive" % versions.sparkVersion exclude("commons-collections", "commons-collections") exclude("commons-logging", "commons-logging") excludeAll ExclusionRule(organization = "org.apache.hadoop"),
  "org.apache.spark" %% "spark-core" % versions.sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % versions.sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % versions.sparkVersion % "provided",
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "com.github.scopt" %% "scopt" % versions.scoptVersion,
  "com.google.guava" % "guava" % "13.0",
  "joda-time" % "joda-time" % versions.jodaTimeVersion,
  "com.beust" % "jcommander" % versions.jcommanderVersion,
  "io.prometheus" % "simpleclient" % versions.prometheusVersion,
  "io.prometheus" % "simpleclient_servlet" % versions.prometheusVersion,
  "io.prometheus" % "simpleclient_pushgateway" % versions.prometheusVersion,

  "mysql" % "mysql-connector-java" % "5.1.25"
)

lazy val etl_framework = project.in(file(".")).
  dependsOn("etl_feed").aggregate("etl_feed").settings(etl_framework_common_settings: _*)

lazy val etl_feed = project.in(file("etl_feed"))
  .settings(etl_framework_common_settings: _*)
  .settings(mainClass in assembly := Some("com.lzd.etlFramework.etl.feed.LaunchETLExecution"))