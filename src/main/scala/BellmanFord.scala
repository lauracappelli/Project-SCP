import java.io.{BufferedWriter, File, FileWriter}

import FunctionCM._
import org.apache.commons.io.FileUtils
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.Map

object BellmanFord {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      //.setMaster("local")
      .setAppName("BellmanFord")

    val bucketName = "s3n://projectscp-daniele"

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    //sc.setCheckpointDir("src/checkpoint")
    sc.setCheckpointDir(bucketName + "/checkpoint")

    //set output folder and input file
    //val inputfile = bucketName+"/smallCities.txt"
    //val inputfile = "src/main/resources/edgeCitiesConnected.txt"
    //val outputFolder = "ResultsGraph"
    val inputfile = bucketName + "/resources/edgeCitiesConnected.txt"

    //lettura del file con suddivisione nelle colonne
    val textFile = sc.textFile(inputfile).map(s => s.split("\t")).persist()

    //variabile che indica se:
    // - calcolare quanti hop servono ad ogni nodo per arrivare alla destinazione
    // - calcolare il peso del cammino minore che collega ogni nodo alla destinazione
    val hop: Int = 0
    //variabile che indica se i nodi del grafo sono numeri interi o se sono citta' nella forma (nomeCitta',stato)
    val cities: Int = 1

    /* ****************************************************************************************************************
      CASO 1 : GRAFO I CUI NODI SONO NUMERI INTERI
    **************************************************************************************************************** */
    if(cities == 0) {

      //DEFINIZIONE SORGENTE E DESTINAZIONE
      def source = 5
      def destination = 1
      checkSourceAndDestinationInt(source, destination, textFile)

      //CALCOLO CAMMINO MINIMO
      val nodes: RDD[(Int,(Int,Int))] = time(camminoMinimoBFInt(sc,textFile,hop,source,destination))

      //STAMPA DEL RISULTATO
      if(hop == 1) {     //cerco gli hop da ogni nodo alla destinazione
        //creo il file su cui scrivere il il buffer writer
        //val outputfile = "src/main/resources/hop-graph20.txt"
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
        val outputFolder = bucketName + "/output"
        FileUtils.deleteDirectory(new File(outputFolder))
        nodes.coalesce(1, shuffle = true).saveAsTextFile(outputFolder)
      }
      else{     //cerco il cammino minimo di un grafo di nodi interi
        //restituisco il percorso dalla sorgente alla destinazione
        buildPathInt(nodes.collectAsMap(), source, destination)
        //FileUtils.deleteDirectory(new File(outputFolder))
        //nodes.saveAsTextFile(outputFolder)
      }
    }

    /* ****************************************************************************************************************
    CASO 2 : GRAFO I CUI NODI SONO CITTA'
    **************************************************************************************************************** */
    else {

      //DEFINIZIONE SORGENTE E DESTINAZIONE
      def source = ("piccata", "IT")
      def destination = ("fenosa", "IT")
      checkSourceAndDestinationCities(source, destination, textFile)

      //CALCOLO CAMMINO MINIMO
      val nodes = time(camminoMinimoBFCities(sc,textFile,source,destination))

      //STAMPA DEL RISULTATO: restituisco il percorso dalla sorgente alla destinazione
      buildPathCities(nodes.collectAsMap(), source, destination)
      //FileUtils.deleteDirectory(new File(outputFolder))
      //nodes.saveAsTextFile(outputFolder)
    }

    //Stop spark context
    sc.stop()

    /* ****************************************************************************************************************
      FINE MAIN
    **************************************************************************************************************** */
  }

}