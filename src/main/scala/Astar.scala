import java.io._

import FunctionCM._
import org.apache.commons.io.FileUtils
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.Map

object Astar {

  def main(args: Array[String]) {

    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
      //.setMaster("local[*]")
      .setAppName("Astar")

    val bucketName = "s3n://projectscp-daniele"
    //val bucketName = "s3n://projectscp-laura"

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    //sc.setCheckpointDir("src/checkpoint")
    sc.setCheckpointDir(bucketName + "/checkpoint")

    //set output folder and input file
    //val outputFolder = "ResultsGraph"
    //val inputfile = "src/main/resources/smallCities.txt"
    //val inputH = "src/main/resources/hop-graph20.txt"
    val inputfile = bucketName + "/resources/smallCities.txt"
    val inputH = bucketName + "/resources/hop-graph20.txt"

    def numCore = 4

    //lettura del file con suddivisione nelle colonne
    val textFile: RDD[Array[String]] = sc.textFile(inputfile)
      .map(s => s.split("\t"))
      .persist(StorageLevel.MEMORY_ONLY_SER)

    //variabile che indica se i nodi del grafo sono numeri interi o se sono citta' nella forma (nomeCitta',stato)
    val cities: Int = 1

    /* ****************************************************************************************************************
          CASO 1 : GRAFO I CUI NODI SONO NUMERI INTERI
    **************************************************************************************************************** */
    if (cities == 0) {

      //DEFINIZIONE SORGENTE E DESTINAZIONE
      def source = 5
      def destination = 1
      checkSourceAndDestinationInt(source, destination, textFile)

      //LETTURA DELL'EURISTICA
      //leggo il file inputH e memorizzo il contenuto all'interno della RDD hValues che ha due componenti per ogni nodo
      // del grafo: l'id del nodo e il suo valore di h_score
      val hValues: RDD[(Int, Int)] = sc.textFile(inputH).map(s => s.split("\t"))
        .map(a => (a(0).toInt, a(1).toInt))
        .partitionBy(new HashPartitioner(numCore))
        .persist(StorageLevel.MEMORY_ONLY_SER)

      //CALCOLO CAMMINO MINIMO
      val (finish,nodes) = time(camminoMinimoAStarInt(sc,textFile,hValues,source,destination))

      //STAMPA DEL RISULTATO
      if(finish == 1) {
        buildPathInt(nodes.map(a => (a._1, (a._2._1, a._2._4))).collectAsMap(), source, destination)
        //FileUtils.deleteDirectory(new File(outputFolder))
        //nodes.saveAsTextFile(outputFolder)
      }
      else
        println("\n\nNon e' presente nel grafo un percorso da " + source + " a " + destination + "\n\n")

    }

    /* ****************************************************************************************************************
    CASO 2 : GRAFO I CUI NODI SONO CITTA'
    **************************************************************************************************************** */
    else {

      //DEFINIZIONE SORGENTE E DESTINAZIONE
      def source = ("Ferrara", "IT")
      def destination = ("Parma", "IT")
      checkSourceAndDestinationCities(source,destination,textFile)

      //CALCOLO CAMMINO MINIMO
      val (finish,nodes) = time(camminoMinimoAStarCities(sc,textFile,source,destination))

      //STAMPA DEL RISULTATO
      if(finish == 1) {
        val nodesMap: Map[(String, String), (Double, (String, String))] = nodes.map {
          case (citta,(g, _, _, pred, _, _, _, _)) => ((citta._1,citta._2),(g,(pred._1,pred._2)))
        }.collectAsMap()

        buildPathCities(nodesMap, source, destination)
        //FileUtils.deleteDirectory(new File(outputFolder))
        //nodes.saveAsTextFile(outputFolder)
      }
      else
        println("\n\nNon e' presente nel grafo un percorso da " + source._1 + " a " + destination._1 + "\n\n")

    }

    //Stop spark context
    sc.stop()

  }

}