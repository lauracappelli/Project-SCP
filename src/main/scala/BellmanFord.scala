import java.io.{BufferedWriter, File, FileWriter}

import FunctionCM._
import org.apache.commons.io.FileUtils
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object BellmanFord {

  def main(args: Array[String]): Unit = {

    /* ****************************************************************************************************************
        IMPOSTAZIONI AMBIENTE LOCALE
    **************************************************************************************************************** */

    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("BellmanFord")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.default.parallelism", "8")

    def numCore = conf.get("spark.default.parallelism").toInt

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir("src/checkpoint")

    //imposto il nome del file di input
    val inputfile = "src/main/resources/graph/graph1.txt"

    //imposto la cartella di output
    val outputFolder = "src/main/resources/CitiesGraph/"

    /* ****************************************************************************************************************
        IMPOSTAZIONI AMBIENTE CLOUD
    **************************************************************************************************************** */
    /*
    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
      .setAppName("BellmanFord")
      .set("spark.default.parallelism", "35")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    def numCore = conf.get("spark.default.parallelism").toInt

    //imposto il nome del bucket
    //val bucketName = "s3n://projectscp-daniele"
    val bucketName = "s3n://projectscp-laura"

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir(bucketName + "/checkpoint")

    //imposto il nome del file di input
    val inputfile = bucketName + "/resources/graph10.txt"

    //imposto la cartella di output
    val outputFolder = bucketName + "/output/bellmanford"
    */
    /* ****************************************************************************************************************
        DEFINIZIONI GENERALI
    **************************************************************************************************************** */
    //lettura del file con suddivisione degli attributi
    val textFile = sc.textFile(inputfile, minPartitions = numCore)
      .map(s => s.replaceAll("[()]", ""))
      .map(s => s.replaceAll(",", "\t"))
      .map(s => s.split("\t"))
      .persist(StorageLevel.MEMORY_ONLY_SER)

    //variabile che indica se:
    // - calcolare quanti hop servono ad ogni nodo per arrivare alla destinazione
    // - calcolare il peso del cammino minore che collega ogni nodo alla destinazione
    val hop: Int = 0
    //variabile che indica se i nodi del grafo sono numeri (interi) o se sono citta' (stringhe)
    val cities: Int = 1

    /* ****************************************************************************************************************
      CASO 1 : GRAFO I CUI NODI SONO NUMERI INTERI
    **************************************************************************************************************** */
    if(cities == 0) {

      //DEFINIZIONE SORGENTE E DESTINAZIONE E LETTURA DEI DATI
      def source = 5
      def destination = 1
      checkSourceAndDestinationInt(source, destination, textFile)
      val edges: RDD[(Int,(Int,Int))] = createIntEdgesRDD(sc,textFile,hop,numCore)

      //CALCOLO CAMMINO MINIMO
      val nodes: RDD[(Int,(Int,Int))] = time(camminoMinimoBFInt(edges,source,numCore))

      //STAMPA DEL RISULTATO
      if(hop == 1) {     //cerco gli hop da ogni nodo alla destinazione
        //creo il file su cui scrivere il il buffer writer
        //val outputfile = "src/main/resources/hop.txt"
        //val file = new File(outputfile)
        //val bw = new BufferedWriter(new FileWriter(file))

        //sposto gli archi ottenuti sul driver, ordinandoli per sorgente crescete
        //val bfSortedNodesArray = nodes.collectAsMap().toSeq.sortBy(_._1)

        //scrivo ciascun arco sul file nel formato (sorgenteArco, destinazioneArco, hop_from_destination)
        /*for (n <- bfSortedNodes) {
          bw.write(n._1 + "\t" + n._2._1 + "\t" + n._2._2 + "\n")
        }
        //chiudo il buffer writer
        bw.close()*/

        //salvo il contenuto di nodes in un file
        FileUtils.deleteDirectory(new File(outputFolder))
        nodes.coalesce(1, shuffle = true).saveAsTextFile(outputFolder)
      }
      else{     //cerco il cammino minimo di un grafo di nodi interi
        //restituisco il percorso dalla sorgente alla destinazione
        buildPathInt(nodes.collectAsMap(), source, destination)

      }
    }

    /* ****************************************************************************************************************
    CASO 2 : GRAFO I CUI NODI SONO CITTA'
    **************************************************************************************************************** */
    else {

      //DEFINIZIONE SORGENTE E DESTINAZIONE E LETTURA DEI DATI
      //definizione manuale di sorgente e destinazione
      /*def source = "pergaccio"
      def destination = "mambrini"
      checkSourceAndDestinationCities(source, destination, textFile)*/
      //lettura dei dati
      val edges: RDD[( String,(String, Float))] = createCitiesEdgesRDD(textFile,numCore)
      //definizione random di sorgente e destinazione
      val randomCities = edges.groupByKey().keys.takeSample(false,2,scala.util.Random.nextLong())
      def source: String = randomCities(0)
      def destination: String = randomCities(1)

      //CALCOLO CAMMINO MINIMO
      val nodes = time(camminoMinimoBFCities(edges,source,numCore))

      //STAMPA DEL RISULTATO: restituisco il percorso dalla sorgente alla destinazione
      buildPathCities(nodes, source, destination)

    }

    //Stop spark context
    sc.stop()

  }

}