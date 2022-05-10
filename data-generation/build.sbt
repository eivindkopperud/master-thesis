name := "data-generation"

version := "0.1"

Global / onChangedBuildSource := ReloadOnSourceChanges
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0" % "provided"

assembly / mainClass := Some("Main")


// https://mvnrepository.com/artifact/org.apache.spark/spark-sql

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-graphx" % "3.2.0" % "provided",
  "com.github.javafaker" % "javafaker" % "1.0.2",
  "org.scalanlp" %% "breeze" % "1.2",
  "org.scalanlp" %% "breeze-viz" % "1.2",
  "org.scalatest" %% "scalatest" % "3.2.11" % "test",
)

Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDF") // Display runtime for tests
assemblyOutputPath in assembly := file("uber.jar")


scalaVersion := "2.12.15"

//https://coderwall.com/p/6gr84q/sbt-assembly-spark-and-you
ThisBuild / assemblyMergeStrategy := {
  case PathList("org", "aopalliance", xs@_*) => MergeStrategy.last
  case PathList("javax", "inject", xs@_*) => MergeStrategy.last
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.last
  case PathList("javax", "activation", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", xs@_*) => MergeStrategy.last
  case PathList("com", "google", xs@_*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.last
  case PathList("com", "codahale", xs@_*) => MergeStrategy.last
  case PathList("com", "yammer", xs@_*) => MergeStrategy.last
  case PathList("jackson", xs@_*) => MergeStrategy.last
  case PathList("io", "netty", xs@_*) => MergeStrategy.first
  case PathList("com", "fasterxml", xs@_*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "META-INF/io.netty.versions.properties" => MergeStrategy.last
  case "git.properties" => MergeStrategy.last
  case "overview.html" => MergeStrategy.last // Added this for 2.1.0 I think
  case x if x endsWith "native-image.properties" => MergeStrategy.last
  case x if x endsWith "module-info.class" => MergeStrategy.discard
  case x if x endsWith "reflection-config.json" => MergeStrategy.last
  case x if x endsWith "Resource$AuthenticationType.class" => MergeStrategy.last
  case x if x endsWith "nowarn$.class" => MergeStrategy.last
  case x if x endsWith "nowarn.class" => MergeStrategy.last
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}
Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated
