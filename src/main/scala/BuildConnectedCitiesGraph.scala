import java.io.{BufferedWriter, File, FileWriter}

import FunctionCM._
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object BuildConnectedCitiesGraph {

  def main(args: Array[String]): Unit = {

    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("CreateCitiesGraph")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir("src/checkpoint")

    //set output folder
    //val outputFolder = "src/main/resources/CitiesGraph"

    /* ****************************************************************************************************************
        PARTE 1 - LETTURA DEL DB DI GEONAMES + FILTRO AL DB PER OTTENERE SOLO LE ENTRIES INTERESSANTI
    **************************************************************************************************************** */

    //istanzio l'RDD che conterra' l'intero database delle citta'
    var db: RDD[((String, String), ((Double,Double),(String,String),String))] = sc.emptyRDD

    //scorro tutti i file leggendo il contenuto e memorizzando solo le entries con significato valido
    for(i <- 1 to 11){

      //imposto il nome del file di input
      val inputfile = "src/main/resources/DB/" + i + "-allCountries.txt"
      val textFile = sc.textFile(inputfile)

      //Per ogni citta' del file memorizzo:
      // (nomeCitta',siglaStato),((latitudine,longitudine),(classe,sottoclasse),continente)
      val partialDB: RDD[((String, String), ((Double,Double),(String,String),String))] = textFile
        //leggo il file suddividendo le colonne
        .map(s => s.split("\t"))
        //tolgo tutte le righe in cui mancano informazioni e quelle in cui latitudine e longitudine non sono numeri
        .filter(s => s.length == 8 && isNumber(s(2)) && isNumber(s(3)))
        //distinguo la chiave dai valori
        .map(a => ((a(1).toLowerCase,a(6)),
        ((a(2).toDouble/100000,a(3).toDouble/100000),(a(4),a(5)),a(7).split("/")(0))))

      //unisco il contenuto del file appena analizzato con il database già calcolato
      db = db.union(partialDB).persist()
    }

    //filtro il db ottenuto mantenendo solo le entries interessanti: citta europee e italiane
    db = db.filter{
      case ((_,stato),(_, sigla, cont)) =>
        sigla._1.equals("P") && (sigla._2.equals("PPL") || sigla._2.equals("PPLC")) && cont.equals("Europe") &&
          stato.equals("IT")
    }
      //se esiste piu' di una citta' con lo stesso nome in uno stesso stato, ne seleziono solo una prestando attenzione
      // di selezionare la citta' piu' importante (capitale)
      .reduceByKey((a,b) => if (a._2._2.contains("PPLC")) a else b)
      .sample(false, 0.05, 0)
      .partitionBy(new HashPartitioner(4))

    //println("\n\nCittà iniziali: " + db.count())
    //db.collect().foreach(println)

    /* ****************************************************************************************************************
        PARTE 1B - LETTURA DEL DB DI GEONAMES GIA' FILTRATO E ORGANIZZATO
    **************************************************************************************************************** */
    /*
        //file contenente il db
        val inputfile = "src/main/resources/1000citta.txt"

        //lettura del file
        val textFile = sc.textFile(inputfile)
        val db: RDD[((String, String), (Int, Double, Double, String))] = textFile.map( s => s.split("\t"))
          .filter(s => s.length == 6 && isNumberfromDB(s(2)) && isNumberfromDB(s(3)) && isNumberfromDB(s(4)))
          .map(s => ((s(0), s(1)),(s(2).toInt, s(3).toDouble, s(4).toDouble, s(5))))
    */
    /* ****************************************************************************************************************
        PARTE 2 - DIVISIONE DELLE CITTA' NEL RETICOLO GEOGRAFICO
    **************************************************************************************************************** */
    //maxLat = 90 e maxLong = 180

    //raggruppo le citta' in base al reticolo geografico nelle quali sono situate. Per ogni reticolo memorizzo:
    // - chiave: coordinate del reticolo nella forma (#lat, #long). (latitudine: da -9 a +8, longitudine: da -18 a +17)
    // - valore: lista delle citta' del reticolo nella forma (Nome citta', sigla stato, latitudine, longitudine)
    val geoRetic: RDD[((Int, Int),Iterable[(String,String,Double,Double)])] = db
      .map{
        case ((citta,stato),(coord, _, _)) =>
          (obtainRetNumber(coord._1, coord._2),(citta, stato, coord._1, coord._2))
      }
      .groupByKey()
      .repartition(4).persist()
    //da guardare qua
    geoRetic.checkpoint()

    /* ****************************************************************************************************************
        PARTE 3 - CREAZIONE DEGLI ARCHI INTERNI AL RETICOLO
    **************************************************************************************************************** */

    //creazione degli archi tra le citta' che, all'interno di un reticolo, distano meno di 100km
    val edgeInsideReticCartesian: RDD[Iterable[((String, String, Double, Double), (String, String, Double, Double))]] =
    //eseguiamo l'operazione di prodotto cartesiano tra gli elementi contenuti in ciascun elemento di geoRetic. In
    // questo modo otteniamo un RDD[Iterable[((String,String, Double, Double), (String,String, Double, Double))]] con
    // un elemento per ogni reticolo che contiene tutte le combinazioni possibili di coppie di citta' appartenenti a
    // quello stesso reticolo
      geoRetic.map( geoReticElem => cartesian(geoReticElem._2))
        //guardare qua
        .persist()

    //Per ogni coppia di ogni valore dell'RDD, calcoliamo la distanza tra le due città. Restituiamo una tripla:
    // ((citta'1,siglaStato), (citta'2,siglaStato), distanza) dove la distanza e' pari al valore calcolato se le due
    // citta' distano meno di 100 km, in caso contrario (o se la coppia e' costituita dalla stessa citta') la
    // distanza vale 1000000000
    val edgeInsideRetic: RDD[Iterable[((String, String, Double, Double), (String, String, Double, Double), Double)]] =
    edgeInsideReticCartesian.map(el => el.map(e => computeDistance(e._1, e._2, 0)))
      //elimino tutti gli archi delle citta' a distanza infinita (=1000000000 )
      .map( el => el.filter( e => e._3 != 1E10 ))
      //guardare qua
      .persist()

    /* ****************************************************************************************************************
        PARTE 4 - CREAZIONE DEGLI ARCHI TRA UN RETICOLO E I VICINI
    **************************************************************************************************************** */

    //per ogni reticolo individuo l'insieme dei reticoli vicini. Creo un RDD cosi' composto:
    // - chiave: id del reticolo nella forma (lat, long)
    // - valore: lista dei reticoli vicini, ognuno dei quali e' identificato dal proprio id nella forma (lat, long).
    //   L'ordine dei vicini e' N,S,E,W
    val neigbours: RDD[((Int,Int),List[(Int,Int)])] = geoRetic.map( ret =>
      (ret._1, List((ret._1._1+1,ret._1._2),(ret._1._1-1,ret._1._2),(ret._1._1,ret._1._2+1),(ret._1._1,ret._1._2 -1))))

    //in ogni reticolo identifico la citta' piu' a N, piu' a S, piu' a E, piu' a O. Ottengo un RDD di forma:
    // - chiave: id del reticolo nella forma (lat, long)
    // - valore: lista delle citta' di confine riportate nella forma (puntoCardinale,(nomeCitta,siglaStato,lat,long)).
    //   L'ordine in cui sono riportate le citta' e' N,S,E,W
    val borderTown: RDD[((Int,Int),List[(Char,(String,String,Double,Double))])] = geoRetic
      .map( el => (el._1, findBorderTown(el._2)))
      //guardare qua
      .repartition(4).cache()
    borderTown.checkpoint()

    //Vogliamo ottenere tutte le informazioni su ogni reticolo necessarie per realizzare gli archi che collegano i
    // reticoli fra loro. Le informazioni necessarie sono: le citta' di confine di un reticolo e le città esterne con
    // le quali le città di confine si devono collegare
    val reticInfo: RDD[((Int,Int),(Iterable[(Char,(String,String,Double,Double))],
      List[(Char,(String,String,Double,Double))]))] =
    //Per prima cosa effettuo una join in modo da ottenere per ogni reticolo l'elenco dei reticoli vicini e le citta'
    // di confine. Ottengo: RDD[((Int,Int),(List[(Int,Int)],List[(Char,(String,String,Double,Double))]))]
    neigbours.join(borderTown)
      //Associamo ad ogni reticolo y vicino del reticolo x in analisi, la citta' del reticolo x con la quale dovra'
      // legarsi; poi raggruppiamo il risultato per chiave in modo da ottenere per ogni reticolo l'elenco delle città
      // alle quali si dovrà collegare. Ottengo RDD[((Int,Int),Iterable[(Char,(String,String,Double,Double))])]
      .flatMap{ case (_, (listaVicini, listaBorderTtown)) => listaVicini.zip(listaBorderTtown) }
      .groupByKey()
      //Associamo ad ogni reticolo, oltre alle citta' esterne con le quali si deve collegare, le sue città di confine
      .join(borderTown)
      //guardare qua
      .repartition(4).cache()
    reticInfo.checkpoint()

    //Una volta ottenute tutte le informazioni necessarie per unire i reticoli, creiamo gli archi che collegano le citta
    // di confine con le città esterne al reticolo
    val edgeBetweenRetic: RDD[Iterable[((String, String, Double, Double), (String, String, Double, Double), Double)]] =
    reticInfo.map{
      //per ogni reticolo identifichiamo chiave, città dei reticoli vicini, città di confine
      case (_, (cittaRetVicini, cittaConfineRet)) =>
        //associamo ad ogni città dei reticoli vicini la corrispondente città del reticolo in analisi e creiamo l'arco
        // utilizzando la funzione computeDistance
        cittaRetVicini.map{
          case('N', citta) => computeDistance(citta, cittaConfineRet.filter( el => el._1 == 'S').head._2, 1)
          case('S', citta) => computeDistance(citta, cittaConfineRet.filter( el => el._1 == 'N').head._2, 1)
          case('E', citta) => computeDistance(citta, cittaConfineRet.filter( el => el._1 == 'W').head._2, 1)
          case('W', citta) => computeDistance(citta, cittaConfineRet.filter( el => el._1 == 'E').head._2, 1)
        }
    }
      //guarda qua
      .repartition(4).cache()
    edgeBetweenRetic.checkpoint()

    //uniamo gli archi interni di ogni reticolo con gli archi che collegano i reticoli fra loro
    val totalEdge: RDD[((String, String, Double, Double), (String, String, Double, Double), Double)] =
      edgeInsideRetic.union(edgeBetweenRetic)
      .flatMap(a => a.map(e => e))
      //guarda qua
      .persist()

    //println("\nTotalEdge")
    //totalEdge.collect().foreach(println)

    //memorizzo sul driver tutti gli archi
    val te = totalEdge.collect()

    println("\n\nCittà rimaste dopo il controllo sulla distanza: " + te.map(a => a._1).distinct.length)
    println("Archi totali: " + te.length)

//DA QUA PARTE DI DANIELE
    //val inputFile = "src/main/resources/edgeCities.txt"

    //Si prende in input il grafo delle città dove ogni arco è costituito da cinque elementi:
    // 1) NOME della prima città estremo dell'arco
    // 2) STATO della della prima città
    // 3) NOME della seconda città estremo dell'arco
    // 4) STATO della seconda città
    // 5) DISTANZA in km tra le due città costituente il peso dell'arco
    //val input = sc.textFile(inputFile)

    //edges è una RDD[(k,v)] nella quale vengono memorizzate, per ogni arco, soltanto i nomi delle due città estremi
    val edges: RDD[((String, String), (String, String))] = totalEdge
        .map (a => ((a._1._1, a._1._2), (a._2._1, a._2._2)))
      .partitionBy(new HashPartitioner(4)).persist()

    //source è la sorgente del grafo scelta in base al numero maggiore di archi uscenti
    val source: (String, String) = edges.map(a => (a._1, 1))
      .reduceByKey(_ + _).reduce((a,b) => if(a._2 > b._2) a else b)._1

    //println("\nSource: " + source)

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
      val fullInput: RDD[((String, String), (Double, Double, String, String, Double, Double, Double))] = totalEdge
        .map(a => ((a._1._1, a._1._2), (a._1._3, a._1._4, a._2._1, a._2._2, a._2._3, a._2._4, a._3)))

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
      val connectedNodes: RDD[((String, String), (Double, Double, String, String, Double, Double, Double))] =
      fullInput.join(discoveredNodes).map{
          case ((a, stateA), ((latA, longA, b, stateB, latB, longB, distance), (_, _))) =>
            ((a, stateA), (latA, longA, b, stateB, latB, longB, distance))
        }

      //scrivo nel file edgeCitiesConnected.txt gli elementi di connectedNodes, in particolare: ogni elemento è un arco
      //e ne viene inserito uno per riga, mentre gli attributi di ogni arco vengono inseriti separati da un TAB

      val connectedNodesArray = connectedNodes.collect()

      for (node <- connectedNodesArray) {
        bw.write(node._1._1 + "\t" + node._1._2 + "\t" + node._2._1 + "\t" + node._2._2 + "\t" + node._2._3 + "\t"
          + node._2._4 + "\t" + node._2._5 + "\t" + node._2._6 + "\t" + node._2._7 + "\n")
      }

      /*
      *******************************************************************************************************
      VERIFICA CONNESSIONE DEL NUOVO GRAFO
      *******************************************************************************************************
       */

      //Si esegue lo stesso procedimento di prima per verificare se il nuovo grafo è effettivamente connesso o meno
      val edgesC: RDD[((String, String), (String, String))] = connectedNodes
        .map(a => ((a._1._1, a._1._2), (a._2._3, a._2._4)))
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

    //save results and stop spark context
    //FileUtils.deleteDirectory(new File(outputFolder))
    //db.saveAsTextFile(outputFolder)
    sc.stop()

  }

}
