name := "spark-simple"

version := "1.0"

scalaVersion := "2.11.9"

val sparkVersion="2.1.0"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % sparkVersion

