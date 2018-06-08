name := "spark-cfs"

organization := "rauljosepalma"

version := "0.1.0_spark_1.6.1-SNAPSHOT"

scalaVersion := "2.10.5"

scalacOptions := Seq("-unchecked", "-deprecation", "-feature")

val sparkVersion = "1.6.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion)
