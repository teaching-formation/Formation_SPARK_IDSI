name := "Formation_SPARK_IDSI"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.8"

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.commons" % "commons-csv" % "1.9.0",
  "com.typesafe" % "config" % "1.4.2",
  "com.google.protobuf" % "protobuf-java" % "3.21.1",
  "org.project-lombok" % "lombok" % "1.18.6",
  "org.slf4j" % "slf4j-api" % "1.7.36",
  "org.slf4j" % "slf4j-log4j12" % "1.7.36"
)



