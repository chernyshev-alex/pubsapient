name := "pubsapient"

version := "0.1"

scalaVersion := "2.13.8"

val sparkVersion = "3.2.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % Test
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.9" % Test
