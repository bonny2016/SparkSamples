name := "SparkSamples"

version := "1.0"

scalaVersion := "2.11.8"


libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.1.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.1.0"
