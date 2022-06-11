package jobs

import org.apache.spark.sql.SparkSession

object Formation1 extends App {

  val spark = SparkSession
    .builder()
    .appName("First Job SPARK")
    .enableHiveSupport()
    .getOrCreate()

  val dataseq = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))
  val rdd = spark.sparkContext.parallelize(dataseq)
  println("Nombre initial de partitions :"+ rdd.getNumPartitions)
  println("Collect :"+ rdd.collect() )
  println("Premier element :"+ rdd.first())
  val vente = spark.read.textFile("C:/sample_ventes.txt")
  //val vente = spark.read.textFile("file:///C:/sample_ventes.txt")
  vente.foreach(f=>{ println(f) })


  val rdd2 = rdd.map(f => { (f._2, (f._1,f._2))} )
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
  val rdd5=spark.sparkContext.parallelize(data)
  val rdd6=rdd.reduceByKey(_ + _)
  rdd6.foreach(println)

  import spark.implicits._
  val columns = Seq("language","users_count")
  //val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
  val rdd7 = spark.sparkContext.parallelize(data)
  val dfFromRDD1 = rdd7.toDF()
  dfFromRDD1.printSchema()
  val dfFromRDD2 = rdd7.toDF("language","users_count")
  dfFromRDD2.printSchema()
  val dfFromRDD3 = spark.createDataFrame(rdd7).toDF(columns:_*)

  import org.apache.spark.sql.types.{StringType, StructField,
    StructType}
  import org.apache.spark.sql.Row
  val schema = StructType( Array(
    StructField("language", StringType,true),
    StructField("users", StringType,true)
  ))
  val rowRDD = rdd7.map(attributes => Row(attributes._1, attributes._2))
  val dfFromRDD4 = spark.createDataFrame(rowRDD,schema)

  import spark.implicits._
  val dfFromData1 = data.toDF()

  val df2 = spark.read.csv("/path_file/file.csv")
}
