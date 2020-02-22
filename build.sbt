name := "spark-school"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.5"

resolvers ++= Seq(
  "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,

  "mrpowers" % "spark-daria" % "0.35.2-s_2.11",
  "MrPowers" % "spark-fast-tests" % "0.20.0-s_2.11" % "test",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)
