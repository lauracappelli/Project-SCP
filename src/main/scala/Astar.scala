import java.io._

import FunctionCM._
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object Astar {

  def main(args: Array[String]) {

    /* ****************************************************************************************************************
        IMPOSTAZIONI AMBIENTE LOCALE
    **************************************************************************************************************** */
    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Astar")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.default.parallelism", "8")

    def numCore = conf.get("spark.default.parallelism").toInt

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir("src/checkpoint")

    //imposto i nomi dei file di input
    val inputfile = "src/main/resources/graph/graph1.txt"
    val inputH = "src/main/resources/hop.txt"

    //imposto la cartella di output
    val outputFolder = "src/main/resources/CitiesGraph/"

    /* ****************************************************************************************************************
        IMPOSTAZIONI AMBIENTE CLOUD
    **************************************************************************************************************** */
    /*
    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
      .setAppName("Astar")
      .set("spark.default.parallelism", "70")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    def numCore = conf.get("spark.default.parallelism").toInt

    //imposto il nome del bucket
    //val bucketName = "s3n://projectscp-daniele"
    val bucketName = "s3n://projectscp-laura"

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir(bucketName + "/checkpoint")

    //imposto i nomi dei file di input
    val inputfile = bucketName + "/resources/graph3.txt"
    val inputH = bucketName + "/resources/hop-graph20.txt"

    //imposto la cartella di output
    val outputFolder = bucketName + "/output/astar"
    */
    /* ****************************************************************************************************************
        DEFINIZIONI GENERALI
    **************************************************************************************************************** */
    //lettura del file con suddivisione nelle colonne
    val textFile: RDD[Array[String]] = sc.textFile(inputfile, minPartitions = numCore)
      .map(s => s.replaceAll("[()]", ""))
      .map(s => s.replaceAll(",", "\t"))
      .map(s => s.split("\t"))
      .persist(StorageLevel.MEMORY_ONLY_SER)

    //variabile che indica se i nodi del grafo sono numeri interi o se sono citta' nella forma (nomeCitta',stato)
    val cities = 1

    /* ****************************************************************************************************************
        CASO 1 : GRAFO I CUI NODI SONO NUMERI INTERI
    **************************************************************************************************************** */
    if (cities == 0) {

      //DEFINIZIONE SORGENTE E DESTINAZIONE E LETTURA DEI DATI
      def source = 5
      def destination = 1
      checkSourceAndDestinationInt(source, destination, textFile)
      val edges: RDD[(Int,(Int,Int))] = createIntEdgesRDD(sc,textFile,0,numCore)

      //LETTURA DELL'EURISTICA
      //leggo il file inputH e memorizzo il contenuto all'interno della RDD hValues che ha due componenti per ogni nodo
      // del grafo: l'id del nodo e il suo valore di h_score
      val hValues: RDD[(Int, Int)] = sc.textFile(inputH).map(s => s.split("\t"))
        .map(a => (a(0).toInt, a(1).toInt))
        .partitionBy(new HashPartitioner(numCore))
        .persist(StorageLevel.MEMORY_ONLY_SER)

      //CALCOLO CAMMINO MINIMO
      val (finish,nodes) = time(camminoMinimoAStarInt(sc,edges,hValues,source,destination,numCore))

      //STAMPA DEL RISULTATO
      if(finish == 1)
        buildPathInt(nodes, source, destination)
      else
        println("\n\nNon e' presente nel grafo un percorso da " + source + " a " + destination + "\n\n")

    }

    /* ****************************************************************************************************************
        CASO 2 : GRAFO I CUI NODI SONO CITTA'
    **************************************************************************************************************** */
    else {

      //DEFINIZIONE SORGENTE E DESTINAZIONE
      def source = "pergaccio"
      def destination = "mambrini"
      checkSourceAndDestinationCities(source,destination,textFile)
      val edges: RDD[((String,Float,Float),((String,Float,Float), Float))] =
        createCompleteCitiesEdgesRDD(textFile,numCore)
      //val randomCities = edges.groupByKey().keys.takeSample(false,2,scala.util.Random.nextLong())
      //def source: String = randomCities(0)._1
      //def destination: String = randomCities(1)._1

      //CALCOLO CAMMINO MINIMO
      val (finish,nodes) = time(camminoMinimoAStarCities(sc,edges,source,destination,numCore))

      //STAMPA DEL RISULTATO
      if(finish == 1)
        buildPathCities(nodes, source, destination)
      else
        println("\n\nNon e' presente nel grafo un percorso da " + source + " a " + destination + "\n\n")

    }

    //Stop spark context
    sc.stop()

  }

}