name := "spark-simple"

version := "1.0"

scalaVersion := "2.11.9"

val sparkVersion = "2.1.0"

resolvers += "apache releases" at "https://repository.apache.org/content/repositories/releases"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % sparkVersion
libraryDependencies += "org.apache.kafka" % "kafka_2.11" % "0.8.2.1"