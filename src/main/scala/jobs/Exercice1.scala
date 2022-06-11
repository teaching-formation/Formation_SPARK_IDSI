package jobs

import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object Exercice1 extends App {
  /* Suppression des messages lieés à l'exécution */
  val _LOGGER = LoggerFactory.getLogger(classOf[App])
  org.apache.log4j.Logger.getLogger("org").setLevel(Level.OFF)
  org.apache.log4j.Logger.getLogger("akka").setLevel(Level.OFF)
  /* Ouverture d'une session spark */
  val spark = SparkSession
    .builder()
    .appName("Exercice 1 job spark")
    .enableHiveSupport()
    .getOrCreate()

  val loup = spark.read.textFile("fichiers/loup.txt")
 /* println("Nombre de partitions : "+ loup.getNumPartitions)*/
  println("Prémière ligne du fichier : " +loup.first())
  loup.collectAsList()
  val moutonLines = loup.filter(line => line.contains("moutons"))
  moutonLines.foreach(f =>println(f))
  /*val loupSplit = loup.flatMap(f=>f.split(" "))
  loupSplit.collectAsList()
  val ocurence = loupSplit.foreach(f => (f,1))
  ocurence.collect()*/

}
