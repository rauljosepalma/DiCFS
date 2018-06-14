name := "spark-cfs"

organization := "rauljosepalma"

version := "0.1.0_spark_2.2.1-SNAPSHOT"

scalaVersion := "2.11.12"

scalacOptions := Seq("-unchecked", "-deprecation", "-feature")

val sparkVersion = "2.2.1"

// Observe that for sramirez package %% is not used to prevent sbt adding
// the scalaVersion after the artifactId. As mentioned in:
// http://stackoverflow.com/questions/36080519/spark-shell-dependencies-translate-from-sbt
libraryDependencies ++= Seq(
  "rauljosepalma" %% "spark-mltools" % "0.1.0_spark_2.2.1-SNAPSHOT",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided")
  // "sramirez" % "spark-MDLP-discretization" % "1.2.1")