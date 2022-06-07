package jobs

import org.apache.spark.sql.SparkSession

object Formation1 extends App {

  val spark = SparkSession
    .builder()
    .appName("Fisrt Job SPARK")
    .enableHiveSupport()
    .getOrCreate()


}
