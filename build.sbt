ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.12.15"
ThisBuild / fork := true

name := "ps2events"

val versions = new {
  val spark = "3.1.2"
  val sttp = "3.5.1"
  val hudi = "0.10.1"
}

libraryDependencies += "org.apache.hudi" %% "hudi-spark3.1.2-bundle" % versions.hudi
//libraryDependencies += "org.apache.spark" %% "spark-avro" % versions.spark
libraryDependencies += "org.apache.spark" %% "spark-sql" % versions.spark % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % versions.spark % "provided"
libraryDependencies += "com.softwaremill.sttp.client3" %% "core" % versions.sttp
libraryDependencies += "com.softwaremill.sttp.client3" %% "httpclient-backend" % versions.sttp
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.1"

assemblyPackageScala / assembleArtifact := false
