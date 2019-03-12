import java.io.{BufferedWriter, FileWriter}

import FunctionCM._
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object DFS {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("DFS")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir("src/Checkpoint")

    val inputFile = "src/main/resources/edgeCities.txt"

    //Si prende in input il grafo delle città dove ogni arco è costituito da cinque elementi:
    // 1) NOME della prima città estremo dell'arco
    // 2) STATO della della prima città
    // 3) NOME della seconda città estremo dell'arco
    // 4) STATO della seconda città
    // 5) DISTANZA in km tra le due città costituente il peso dell'arco
    val input = sc.textFile(inputFile)

    //edges è una RDD[(k,v)] nella quale vengono memorizzate, per ogni arco, soltanto i nomi delle due città estremi
    val edges: RDD[((String, String), (String, String))] = input.map(s => s.split("\t"))
      .map(a => ((a(0), a(1)), (a(2), a(3))))
      .partitionBy(new HashPartitioner(4)).persist()

    //val source = edgesWithIndex.filter(a => a._2._2 == 0).map(a => a._1).cache().take(1).head

    //source è la sorgente del grafo scelta in base al numero maggiore di archi uscenti
    val source: (String, String) = edges.map(a => (a._1, 1))
      .reduceByKey(_ + _).reduce((a,b) => if(a._2 > b._2) a else b)._1

    //nodes è una RDD[(k,v)] nella quale sono presenti i nodi del grafo. Per ogni nodo vengono memorizzati tre attributi:
    // 1) NOME della città-nodo
    // 2) valore booleano per indicare se il nodo è stato SCOPERTO oppure no. Un nodo diventa scoperto quando si
    // visitano tutti i suoi vicini
    // 3) intero che può assumere tre valori 0, 1, 2, utilizzato per gestire la VISITA dei nodi durante la computazione.
    // Il valore 0 indica che il nodo non è stato ancora visitato, il valore 1 indica che il nodo è stato visitato
    // durante la visita di un suo vicino, mentre il valore 2 indica che il nodo è stato scelto come nodo inziale da
    // cui far partire la visita dei suoi vicini
    var nodes: RDD[((String, String), (Boolean, Int))] = edges.groupByKey()
      .map {
        case (k, _) =>
          if(k == source) {
            (k, (false, 1))
          }
          else {
            (k, (false, 0))
          }
      }.repartition(4).cache()

    /*
    ***********************************************************************************************************
    VERIFICA CONNESSIONE GRAFO
    ***********************************************************************************************************
     */

    //visitedNodes è una RDD[(k,v)] costruita a partire da nodes, nella quale vengono memorizzati soltanto i nodi
    // visitati nell'iterazione precedente. Questi nodi hanno il terzo valore uguale a 1
    var visitedNodes: RDD[((String, String), (Boolean, Int))] = nodes.filter(a => a._2._2 == 1).repartition(4).cache()

    //Il ciclo continua finché l'RDD visitedNodes non è vuota. Diventa vuota quando tutti i nodi del grafo o quelli
    // appartenti ad una componente connessa sono stati visitati e successivamente scoperti
    while (!visitedNodes.isEmpty()) {

      //FASE 1 - aggiornamento nodi visitati
      //Si modifica da 1 a 2 il valore dei nodi scelti come nodi iniziali da cui far partire la visita dei loro vicini
      nodes = nodes.map {
        case (b, (false, 1)) => (b, (false, 2))
        case (b, (discovered, visited)) => (b, (discovered, visited))
      }

      //discoveredNodes è una RDD[(k,v)] contenente soltanto i nodi scelti nel passo precedente
      val discoveredNodes: RDD[((String, String), (Boolean, Int))] = nodes.filter {
        case (_, (false, 2)) => true
        case _ => false
      }

      //FASE 2 - aggiornamento vicini
      //OPERAZIONE 1: edges.join(discoveredNodes)
      // ad ogni arco del grafo associo gli attributi del nodo scoperto costituente il primo estremo dell'arco stesso.
      // Ottengo una RRD composta dai seguenti valori:
      // (nodo-chiave, (secondo-nodo, (nodo-chiave-scoperto, nodo-chiave-visitato))
      //OPERAZIONE 2: risultatoOperazione1.map(...)
      // scambio il nodo-chiave con il secondo-nodo in modo tale da prendere gli attributi anche dei nodi vicini ai
      // nodi scoperti
      //OPERAZIONE 3: risultatoOperazione2.join(nodes)
      // ad ogni arco in cui il primo nodo estremo è un vicino di un nodo scoperto associo i suoi attributi memorizzati
      // in nodes e ottengo la seguente struttura:
      // (secondo-nodo, ((nodo-chiave, (nodo-chiave-scoperto, nodo-chiave-visitato)), (secondo-nodo-scoperto, secondo-nodo-visitato))
      //OPERAZIONE 4: risultatoOperazione3.map(...)
      // ad ogni nodi vicino aggiorno il suo valore visitato a 1
      val updateNodes: RDD[((String, String), (Boolean, Int))] = edges.join(discoveredNodes)
        .map {
          case (a, (b, info)) => (b, (a, info))
        }
        .join(nodes)
        .map {
          case (b, ((a, infoA), (discovered, _))) => (b, (discovered, 1))
        }

      //FASE 3 - REDUCE
      //Effettuo l'unione tra le RDD nodes e updateNodes ottenuta al passo precedente e accorpo i valori con la stessa
      //chiave tenendo soltanto quelli il cui valore visitato è maggiore
      nodes = nodes.union(updateNodes).reduceByKey((a,b) => if(a._2 > b._2) a else b).repartition(4).cache()
      //nodes.checkpoint()

      //FASE 4 - aggiornamento nodi scoperti
      //Si modifica da false a true il secondo attributo dei nodi che sono stati scoperti, cioè quelli in cui tutti
      //i vicini sono stati visitati
      nodes = nodes.map {
        case (b, (false, 2)) => (b, (true, 2))
        case (b, (discovered, visited)) => (b, (discovered, visited))
      }

      //FASE 5 - aggiornamento nodi visitati
      //Si aggiorna l'RDD visitedNodes con i nuovi nodi visitati nell'iterazione appena terminata
      visitedNodes = nodes.filter(a => a._2._2 == 1).repartition(4).cache()

    }

    //disconnectedNodes rappresenta il numero dei nodi non visitati al termine dell'algoritmo.
    //Se il loro numero è zero, allora il grafo è connesso, se è uno, il grafo non è connesso
    val disconnectedNodes = nodes.filter(a => !a._2._1).count().toInt

    if(disconnectedNodes == 0) {
      println("\n\nIl grafo è connesso\n\n")
    }
    else {
      println("\n\nIl grafo non è connesso\nSono presenti " + disconnectedNodes + " nodi disconnessi" + "\n\n")

      //println("Nodes")
      //nodes.collect().foreach(println)

      /*
      *******************************************************************************************************
      CREAZIONE DEL GRAFO CONNESSO A PARTIRE DAL GRAFO NON CONNESSO
      *******************************************************************************************************
       */

      //Se il grafo non è connesso, quindi se disconnectedNodes è maggiore di zero, si eliminano dal grafo gli archi
      //i cui nodi estremi sono sconnessi e si scrive il nuovo grafo sul file edgeCitiesConnected.txt
      val outputFile = "src/main/resources/edgeCitiesConnected.txt"
      val bw = new BufferedWriter(new FileWriter(outputFile))

      //per scrivere sul file il nuovo grafo connesso ho bisogno di tutti gli attributi degli archi del grafo di
      //partenza. Questi attributi li memorizzo nell'RDD[(k,v)] fullInput
      val fullInput: RDD[((String, String), (String, String, Double))] = input.map(s => s.split("\t"))
        .map(a => ((a(0), a(1)), (a(2), a(3), a(4).toDouble)))

      //discoveredNodes contiene soltatno i nodi scoperti del grafo di partenza, cioè quelli che hanno il secondo
      // attributo uguale a TRUE nell'RDD nodes
      val discoveredNodes: RDD[((String, String), (Boolean, Int))] = nodes.filter(a => a._2._1)

      //connectedNodes contiene solamente gli archi in cui il primo nodo estremo è un nodo scoperto, così da ottenere
      //un grafo connesso.
      //Il controllo viene effettuato soltanto sul primo nodo estremo e non anche sul secondo poiché se il primo nodo
      //estremo dell'arco è sconnesso lo sarà anche il secondo e di conseguenza l'arco non farà parte della componente
      //connessa del grafo.
      //L'operazione di JOIN, fullInput.join(discoveredNodes), restituisce solamente gli archi appartenenti alla
      //omponente connessa del grafo poiché nel join si legano le chiavi comuni alle due RDD
      val connectedNodes: RDD[((String, String), (String, String, Double))] = fullInput.join(discoveredNodes)
        .map{
          case ((a, stateA), ((b, stateB, distance), (_, _))) =>
            ((a, stateA), (b, stateB, distance))
        }

      //scrivo nel file edgeCitiesConnected.txt gli elementi di connectedNodes, in particolare: ogni elemento è un arco
      //e ne viene inserito uno per riga, mentre gli attributi di ogni arco vengono inseriti separati da un TAB
      for (node <- connectedNodes.collect()) {
        bw.write(node._1._1 + "\t" + node._1._2 + "\t" + node._2._1 + "\t" + node._2._2 + "\t" + node._2._3 + "\n")
      }


      /*
      *******************************************************************************************************
      VERIFICA CONNESSIONE DEL NUOVO GRAFO
      *******************************************************************************************************
       */

      //Si esegue lo stesso procedimento di prima per verificare se il nuovo grafo è effettivamente connesso o meno
      val edgesC: RDD[((String, String), (String, String))] = connectedNodes
        .map(a => ((a._1._1, a._1._2), (a._2._1, a._2._2)))
        .partitionBy(new HashPartitioner(4)).persist()

      val sourceC: (String, String) = edgesC.map(a => (a._1, 1))
        .reduceByKey(_ + _).reduce((a,b) => if(a._2 > b._2) a else b)._1

      var nodesC: RDD[((String, String), (Boolean, Int))] = edgesC.groupByKey()
        .map {
          case (k, _) =>
            if(k == sourceC) {
              (k, (false, 1))
            }
            else {
              (k, (false, 0))
            }
        }.repartition(4).cache()

      var visitedNodesC: RDD[((String, String), (Boolean, Int))] = nodesC.filter(a => a._2._2 == 1).repartition(4).cache()

      while (!visitedNodesC.isEmpty()) {

        nodesC = nodesC.map {
          case (b, (false, 1)) => (b, (false, 2))
          case (b, (discovered, visited)) => (b, (discovered, visited))
        }

        val discoveredNodesC: RDD[((String, String), (Boolean, Int))] = nodesC.filter {
          case (_, (false, 2)) => true
          case _ => false
        }

        val updateNodesC = edgesC.join(discoveredNodesC)
          .map {
            case (a, (b, info)) => (b, (a, info))
          }
          .join(nodesC)
          .map {
            case (b, ((a, infoA), (discovered, _))) => (b, (discovered, 1))
          }

        nodesC = nodesC.union(updateNodesC).reduceByKey((a,b) => if(a._2 > b._2) a else b).repartition(4).cache()
        //nodes.checkpoint()

        nodesC = nodesC.map {
          case (b, (false, 2)) => (b, (true, 2))
          case (b, (discovered, visited)) => (b, (discovered, visited))
        }

        visitedNodesC = nodesC.filter(a => a._2._2 == 1).repartition(4).cache()
      }

      val disconnectedNodesC = nodesC.filter(a => !a._2._1).count().toInt

      //println("\nNodesC")
      //nodesC.collect().foreach(println)

      if(disconnectedNodesC == 0) {
        println("Il grafo è connesso\nCittà presenti nel grafo connesso: " + nodesC.count() + "\n\n")
      }
      else {
        println("Il grafo non è connesso\nSono presenti " + disconnectedNodesC + " nodi disconnessi" + "\n\n")
      }

      bw.close()
    }

    sc.stop()

  }
}
