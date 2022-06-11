package jobs

import org.apache.spark.sql.SparkSession

import org.slf4j.LoggerFactory

import org.apache.log4j.Level

object Formation1 extends App {

  val _LOGGER = LoggerFactory.getLogger(classOf[App])
  org.apache.log4j.Logger.getLogger("org").setLevel(Level.OFF)
  org.apache.log4j.Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession
    .builder()
    .appName("Fisrt Job SPARK")
    .enableHiveSupport()
    .getOrCreate()

  val dataSeq = Seq(("Java", 20000), ("Python", 100000),("Scala", 3000))
  val rdd=spark.sparkContext.parallelize(dataSeq)
  println(" Nombre initial de partitions :"+ rdd.getNumPartitions)
  println(" Collect :"+ rdd.collect() )
  println(" Premier element :"+ rdd.first() )
  val rdd3 = rdd.sortByKey(false, 6)
  val rdd4 = rdd.reduce( (a,b)=> ("min",a._2 min b._2))._2
  val data = Seq(("Project", 1),
    ("Gutenberg’s", 1),
    ("Alice’s", 1),
    ("Adventures", 1),
    ("in", 1),
    ("Wonderland", 1),
    ("Project", 1),
    ("Gutenberg’s", 1),
    ("Adventures", 1),
    ("in", 1),
    ("Wonderland", 1),
    ("Project", 1),
    ("Gutenberg’s", 1))

  println("les 2lements de data"+data)

}
