package jobs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row

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

  import spark.implicits._

  val dataSeq = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))
  val rdd = spark.sparkContext.parallelize(dataSeq)
  println("Nombre initial de partitions :"+ rdd.getNumPartitions)
  println("Collect :"+ rdd.collect() )
  println("Premier element :"+ rdd.first())


  val vente = spark.read.textFile("C:/sample_ventes.txt")
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

  val rdd1=spark.sparkContext.parallelize(data)
  val rdd8=rdd1.reduceByKey(_ + _)
  rdd8.foreach(println)


  import spark.implicits._
  val columns = Seq("language","users_count")
  val data1 = Seq(("Java", "20000"), ("Python", "100000"),
    ("Scala", "3000"))
  val rdd7 = spark.sparkContext.parallelize(data1)
  val dfFromrdd1 = rdd7.toDF()
  dfFromrdd1.printSchema()
  val dfFromrdd3 = rdd7.toDF("language","users_count")
  dfFromrdd3.printSchema()
  val dfFromrdd2 = spark.createDataFrame(rdd).toDF(columns:_*)

 /* val schema = StructType( Array(
    StructField("language", StringType,true),
    StructField("users", StringType,true) ))
  val rowrdd = rdd.map(attributes => Row(attributes._1, attributes._2))
  val dfFromrdd4 = spark.createDataFrame(rowrdd,schema)

  val dfFromData1 = data.toDF()
  val df2 = spark.read.csv("C:/sample_ventes.txt") */

  val lignesAvecPoupee = vente.filter(line => line.contains("Minneapolis"))
  lignesAvecPoupee.take(2)
  val longueursLignes = vente.map(l => l.length)

  println("********************* FIN DU JOB ***********************")


}
