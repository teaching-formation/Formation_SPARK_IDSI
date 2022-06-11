package jobs

import org.apache.spark.sql.SparkSession

object Formation1 extends App {

  val spark = SparkSession
    .builder()
    .appName("Fisrt Job SPARK")
    .enableHiveSupport()
    .getOrCreate()

  val dataSeq = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
  val rdd = spark.sparkContext.parallelize(dataSeq)
  println("Nombre initial de partotions:"+ rdd.getNumPartitions)
  println("Collect:"+ rdd.collect())
  println("Premier element":+ rdd.first() )
  val rdd2 = rdd.map(f => { (f._2, (f._1,f._2))} )
  val rdd3 = rdd.sortByKey(false,6)
  val data = Seq(("Project", 1), ("Gutenberg’s", 1),
    ("Alice’s", 1), ("Adventures", 1), ("in", 1),
    ("Wonderland", 1), ("Project", 1), ("Gutenberg’s", 1),
    ("Adventures", 1), ("in", 1), ("Wonderland", 1),
    ("Project", 1), ("Gutenberg’s", 1))
  val rdd5 = spark.sparkContext.parallelize(data)

  val rddb = rdd5.reduceByKey(_ + _)
  rddb.foreach(println)

  import spark.implicits._
  val columns = Seq("language","users_count")
  val datab = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
  val rdd6 = spark.sparkContext.parallelize(datab)
  val dfFromRDD1 = rdd6.toDF()
  dfFromRDD1.printSchema()
  val dfFromRDD2 = rdd6.toDF("language","users_count")
  dfFromRDD2.printSchema()
  val dfFromRDD3 = spark.createDataFrame(rdd6).toDF(columns:_*)

  import org.apache.spark.sql.types.{StringType, StructField, StructType}
  import org.apache.spark.sql.Row
  val schema = StructType( Array( StructField("language", StringType,true),
    StructField("users", StringType,true) ))
  val rowRDD = rdd6.map(attributes => Row(attributes._1, attributes._2))
  val dfFromRDD4 = spark.createDataFrame(rowRDD,schema)

  import spark.implicits._
  val dfFromData1 = datab.toDF()


}