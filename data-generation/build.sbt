name := "data-generation"

version := "0.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-graphx" % "3.2.0",
  "com.github.javafaker" % "javafaker" % "1.0.2",
  "org.scalanlp" %% "breeze" % "1.2",
  "org.scalanlp" %% "breeze-viz" % "1.2",
  "org.scalatest" %% "scalatest" % "3.2.11" % "test",
)

Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDF") // Display runtime for tests


scalaVersion := "2.12.15"
