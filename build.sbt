ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.2.0"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
val provided = "provided"
val hdp = ("org.apache.hadoop", "2.10.1")

libraryDependencies ++= Seq(
  hdp._1 % "hadoop-common" % hdp._2 %
    provided, hdp._1 % "hadoop-hdfs" % hdp._2 %
    provided,
  hdp._1 % "hadoop-mapreduce-client-core" % hdp._2 % provided
)

lazy val root = (project in file("."))
  .settings(
    name := "ScalaMapReduce"
  )