package jobs

import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object Formation1 extends App {
  val _LOGGER = LoggerFactory.getLogger(classOf[App])
  org.apache.log4j.Logger.getLogger("org").setLevel(Level.OFF)
  org.apache.log4j.Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession
    .builder()
    .appName("Second Job SPARK")
    .enableHiveSupport()
    .getOrCreate()



  println("8 + 3 = "+(8+3))
  println("1 + 7 * 2 = "+(1 + 7 * 2))
  println(" (1 + 7) * 2 = "+( (1 + 7) * 2))
  println(" (12 % 3) * (5 / 2). = "+( (12 % 3) * (5 / 2)))


}
