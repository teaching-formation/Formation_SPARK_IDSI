name := "Formation_SPARK_IDSI"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.8"

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.commons" % "commons-csv" % "1.1"




)



