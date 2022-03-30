ThisBuild / version := "0.2.0"
ThisBuild / scalaVersion := "2.13.8"
ThisBuild / fork := true
Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated
Compile / runMain := Defaults.runMainTask(Compile / fullClasspath, Compile / run / runner).evaluated

name := "ps2events"

val versions = new {
  val spark = "3.2.1"
  val sttp = "3.5.1"
  val hadoop = "3.3.1"
}

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-aws" % versions.hadoop % "provided",
  "org.apache.spark" %% "spark-avro" % versions.spark % "provided",
  "org.apache.spark" %% "spark-hadoop-cloud" % versions.spark % "provided",
  "org.apache.spark" %% "spark-sql" % versions.spark % "provided",
  "org.apache.spark" %% "spark-streaming" % versions.spark % "provided",
  "com.github.scopt" %% "scopt" % "4.0.1",
  "com.softwaremill.sttp.client3" %% "core" % versions.sttp,
  "com.softwaremill.sttp.client3" %% "httpclient-backend" % versions.sttp,
)

assemblyPackageScala / assembleArtifact := false
