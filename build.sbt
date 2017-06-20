name := "spark-cfs"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.8"

scalacOptions := Seq("-unchecked", "-deprecation", "-feature")

val sparkVersion = "2.1.0"

// Observe that for sramirez package %% is not used to prevent sbt adding
// the scalaVersion after the artifactId. As mentioned in:
// http://stackoverflow.com/questions/36080519/spark-shell-dependencies-translate-from-sbt
libraryDependencies ++= Seq(
  "rauljosepalma" %% "spark-mltools" % "0.1.0-SNAPSHOT",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided")
  // "sramirez" % "spark-MDLP-discretization" % "1.2.1")


// This was done according to the book "sbt in Action" to build a fat jar
// with the assembly task. A pluging was also added to plugins.sbt
import AssemblyKeys._
assemblySettings