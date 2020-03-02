name := "spark-school"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.4"

resolvers ++= Seq(
  "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
)


libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"

libraryDependencies += "mrpowers" % "spark-daria" % "0.35.2-s_2.11"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.1.2"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.1.2"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.1"

libraryDependencies += "MrPowers" % "spark-fast-tests" % "0.20.0-s_2.11" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
