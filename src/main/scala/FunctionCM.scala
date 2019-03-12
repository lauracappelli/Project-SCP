import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import java._
import scala.collection.Map

object FunctionCM {

  def camminoMinimoAStarInt(sc: SparkContext, textFile: RDD[Array[String]], hValues: RDD[(Int,Int)],
                            source: Int, destination: Int): (Int, RDD[(Int,(Int,Int,Int,Int,Int,Int,(Int,Int)))]) = {

    //LETTURA DEL FILE E CREAZIONE DELL'RDD DEGLI ARCHI
    //read input file and save content in edges: RDD[(Int, (Int, Int))] where: [(destination, (source, weight))]
    val edgesRDD: RDD[(Int, (Int, Int))] = textFile.map(a => (a(0).toInt, (a(1).toInt, a(2).toInt)))
      .partitionBy(new HashPartitioner(4)).persist()
    /*//Because the graph is undirected, read each edge twice: first "n1->n2", then "n2->n1"
    val edgesRDD1: RDD[(Int, (Int, Int))] = textFile.map(a => (a(1).toInt, (a(0).toInt, a(2).toInt)))
    val edgesRDD2: RDD[(Int, (Int, Int))] = textFile.map(a => (a(1).toInt, (a(0).toInt, a(2).toInt)))
    val edgesRDD: RDD[(Int, (Int, Int))] = edgesRDD1.union(edgesRDD2)
      .partitionBy(new HashPartitioner(4)).persist()*/

    //creo un RDD[(k,v)] per ogni nodo del grafo. Ogni elemento dell'RDD ha la forma:
    // nodeId, (g(n), h(n), f(n), predecessore, openSet, closedSet, (xMin, gMin))
    // La coppia (xMin, gMin) e' costituita dall'id e dal g_score del nodo selezionato
    // da openSet con il valore di f_score minore di tutti
    var nodes: RDD[(Int, (Int, Int, Int, Int, Int, Int, (Int, Int)))] = edgesRDD.groupByKey()
      .join(hValues).map {
      case (k,(_,h)) =>
        if(k != source)
          (k, (1000000000, h, 1000000000+h, -1, 0, 0, (-1, 1000000000)))
        else
          (k, (0, h, h, -1, 1, 0, (-1, 1000000000)))
    }.repartition(4).cache()

    //openSet e' l'insieme dei nodi vicini non ancora analizzati
    var openSet: RDD[(Int, (Int, Int, Int, Int, Int, Int, (Int, Int)))] = nodes.filter(a=> a._2._5 != 0)
      .repartition(4).cache()

    var finish = 0
    while(!openSet.isEmpty() && finish == 0) {

      //tra tutti i nodi contenuti in openSet, considero quello con f(n) minore
      val (xId, (gx, hx, fx, px, osx, csx, x)) = openSet.reduce((a, b) => if (a._2._3 < b._2._3) a else b)

      //in xMin e' memorizzato l'id e il g_score del nodo selezionato da openSet con il valore di f_score minore
      // di tutti
      val xMin: RDD[(Int, Int)] = sc.parallelize(Seq((xId, gx))).repartition(4).cache()

      //se il nodo selezionato da openSet e' la destinazione, l'algoritmo termina restituendo il percorso minore
      if (xId == destination) {
        finish = 1
      }
      else {
        //si effettua il prodotto cartesiano tra l'RDD xMin e nodes in modo tale da inserire, con una map, i valori
        // correnti di xMin e gMin presenti all'interno dell'ultima coppia di nodes.
        // Il prodotto cartesiano tra due RDD crea una coppia di due elementi:
        // - il PRIMO e' costituito dal contenuto della prima RDD
        // - il SECONDO e' costituito dal contenuto della seconda RDD
        //Successivamente, rimuovo da openSet il nodo x selezionato e lo aggiungo a closedSet
        nodes = nodes.cartesian(xMin)
          .map { case ((k, (g, h, f, p, os, cs, _)), min) => (k, (g, h, f, p, os, cs, min)) }
          .map { case (k, (g, h, f, p, os, cs, (kMin, gMin))) =>
            if (k == kMin) {
              (k, (g, h, f, p, 0, 1, (kMin, gMin)))
            }
            else {
              (k, (g, h, f, p, os, cs, (kMin, gMin)))
            }
          }

        //seleziono tutti gli y vicini del nodo x escludendo i vicini che appartengono a closedSet, cioe' quelli che
        // hanno il valore cs uguale a 0 e li memorizzo in un RDD[(k,v)] con le seguenti informazioni:
        // y, ((xId, pesoArco), (g(y), h(y), f(y), predecessore, openSet, closedSet, (xMin, g(x))))
        val neighbours = edgesRDD.join(nodes).filter { case (_, ((id, _), (_, _, _, _, _, cs, (kMin, _)))) =>
          if((id == kMin) && (cs == 0)) {
            true
          }
          else {
            false
          }
        }

        //considero ogni vicino del nodo x e, per ognuno di essi, calcolo un valore di g_score come tentativo sommando
        // il valore di g_score del nodo x (gMin) con il peso dell'arco che collega x al nodo vicino (weight).
        // Per ogni nodo vicino effettuo i seguenti controlli:
        //1) Se il nodo non appartiene all'insieme openSet, quindi ha il valore os pari a 0, lo inserisco
        // nell'insieme e aggiorno le sue componenti g_score e f_score con il nuovo valore tentative_g_score e
        // predecessore con l'id del nodo x
        //2) Se il nodo appartiene all'insieme openSet, quindi ha il valore os pari a 1, controllo se
        // tentative_g_score e' minore del valore di g_score del nodo vicino (gY), se lo e' aggiorno le componenti
        // g_score, f_score e predessore come nel caso precedente, altrimenti le lascio invariate
        val updateNodes: RDD[(Int, (Int, Int, Int, Int, Int, Int, (Int, Int)))] = neighbours.map {
          case (yId, ((sourceId, weight), (gy, hy, fy, py, 0, cs, (kMin, gMin)))) =>
            (yId, (gMin + weight, hy, gMin + weight + hy, sourceId, 1, cs, (kMin, gMin)))
          case (yId, ((sourceId, weight), (gy, hy, fy, py, 1, cs, (kMin, gMin)))) =>
            if(gMin + weight < gy) {
              (yId, (gMin + weight, hy, gMin + weight + hy, sourceId, 1, cs, (kMin, gMin)))
            }
            else {
              (yId, (gy, hy, fy, py, 1, cs, (kMin, gMin)))
            }
        }

        //una volta terminata l'analisi dei nodi vicini di x effettuata al passo precedente, l'RDD updateNodes
        // che li contiene viene unita a nodes accorpando con la reduceByKey i valori con la stessa chiave, prendendo
        // soltanto quelli che hanno l'f_score minore.
        //Di seguito aggiorno l'insieme openSet con i nuovi nodi ottenuti dalle analisi precedenti
        nodes = nodes.union(updateNodes).reduceByKey((a,b) => if(a._3 < b._3) a else b).repartition(4).cache()
        nodes.checkpoint()
        openSet = nodes.filter(a => a._2._5 != 0).repartition(4).cache()

      }
    }

    (finish,nodes)
  }

  def camminoMinimoAStarCities(sc:SparkContext,textFile:RDD[Array[String]],source:(String,String),
                               destination:(String,String)):
  (Int, RDD[((String, String, Double, Double), (Double, Double, Double,
    (String, String, Double, Double), Int, Int, ((String, String, Double, Double), Double),
    (String, String, Double, Double)))]) = {

    //leggo il file in input e salvo gli archi fra le citta' nella forma: (citta1,(citta2, distanza)
    val edgesRDD: RDD[((String,String,Double,Double),((String,String,Double,Double), Double))] = textFile
      .map(line => ((line(0),line(1),line(5).toDouble,line(6).toDouble),
        ((line(2),line(3),line(7).toDouble,line(8).toDouble),line(4).toDouble)) )
      .partitionBy(new HashPartitioner(4)).persist()
    //edgesRDD.collect().foreach(println)

    //memorizzo l'insieme dei nodi nella forma
    // citta, elenco di citta vicine alla prima ciascuna riportata con la distanza che la separa dalla prima citta
    val allNodes = edgesRDD.groupByKey().persist()

    //creo un RDD[(k,v)] per ogni nodo del grafo. Ogni elemento dell'RDD ha la forma:
    // citta, (g(n), h(n), f(n), cittaPredecessore, openSet, closedSet, (cityMin, gMin), cittaDestinazione)
    // La coppia (xMin, gMin) e' costituita dall'id e dal g_score del nodo selezionato
    // da openSet con il valore di f_score minore di tutti
    var nodes: RDD[((String, String, Double, Double), (Double, Double, Double, (String, String, Double, Double),
      Int, Int, ((String, String, Double, Double), Double), (String, String, Double, Double)))] = allNodes
      .cartesian(allNodes.keys.filter(c => (c._1,c._2) == destination))
      .map {
        case ((city,_),dest) =>
          if ((city._1, city._2) != source)
            (city, (1E9, 0.0, 1E9, ("", "", 0.0, 0.0), 0, 0, (("", "", 0.0, 0.0), 1E9), dest))
          else {
            val h = getDistanceFromLatLonInKm(city._3, city._4, dest._3, dest._4)
            (city, (0.0, h, 0.0 + h, ("", "", 0.0, 0.0), 1, 0, (("", "", 0.0, 0.0), 1E9), dest))
          }
      }.repartition(4).cache()
    //nodes.collect().foreach(println)

    //openSet e' l'insieme dei nodi vicini non ancora analizzati
    var openSet = nodes.filter(a=> a._2._5 != 0)
      .repartition(4).cache()
    //openSet.collect().foreach(println)

    var finish = 0
    while(!openSet.isEmpty() && finish == 0) {

      //tra tutti i nodi contenuti in openSet, considero quello con f(n) minore
      val (xId, (gx, hx, fx, predx, osx, csx, x, destx)) = openSet.reduce((a, b) => if (a._2._3 < b._2._3) a else b)

      //in xMin e' memorizzato l'id e il g_score del nodo selezionato da openSet con il valore di f_score minore
      // di tutti
      val xMin: RDD[((String, String, Double, Double), Double)] = sc.parallelize(Seq((xId, gx)))
        .repartition(4).cache()

      //se il nodo selezionato da openSet e' la destinazione, l'algoritmo termina restituendo il percorso minore
      if ((xId._1,xId._2) == destination) {
        finish = 1
      }
      else {
        //si effettua il prodotto cartesiano tra l'RDD xMin e nodes in modo tale da aggiornare i valori di xMin e gMin
        // presenti in nodes. Il prodotto cartesiano origina una coppia composta da:
        // - PRIMO elemento: contenuto della prima RDD (nodes)
        // - SECONDO elemento: contenuto della seconda RDD (xMin)
        //Successivamente, rimuovo da openSet il nodo x selezionato e lo aggiungo a closedSet
        nodes = nodes.cartesian(sc.parallelize(Seq((xId, gx)))) //nodes = nodes.cartesian(xMin)
          .map { case ((k, (g, h, f, p, os, cs, _, d)), min) => (k, (g, h, f, p, os, cs, min, d)) }
          .map { case (k, (g, h, f, p, os, cs, (kMin, gMin), d)) =>
            if ((k._1,k._2) == (kMin._1,kMin._2)) {
              (k, (g, h, f, p, 0, 1, (kMin, gMin), d))
            }
            else {
              (k, (g, h, f, p, os, cs, (kMin, gMin), d))
            }
          }

        //seleziono tutti gli y vicini del nodo x escludendo i vicini che appartengono a closedSet, cioe' quelli che
        // hanno il valore cs uguale a 0 e li memorizzo in un RDD[(k,v)] con le seguenti informazioni:
        // y, ((xId, pesoArco), (g(y), h(y), f(y), predecessore, openSet, closedSet, (xMin, g(x)), dest))
        val neighbours = edgesRDD.join(nodes).filter { case (_, ((id, _), (_, _, _, _, _, cs, (kMin, _), _))) =>
          if(((id._1,id._2) == (kMin._1,kMin._2)) && (cs == 0)) true
          else false
        }

        //considero ogni vicino del nodo x e, per ognuno di essi, calcolo un valore di g_score come tentativo sommando
        // il valore di g_score del nodo x (gMin) con il peso dell'arco che collega x al nodo vicino (weight).
        // Per ogni nodo vicino effettuo i seguenti controlli:
        //1) Se il nodo non appartiene all'insieme openSet, quindi ha il valore os pari a 0, lo inserisco
        // nell'insieme e aggiorno le sue componenti g_score e f_score con il nuovo valore tentative_g_score e
        // predecessore con l'id del nodo x
        //2) Se il nodo appartiene all'insieme openSet, quindi ha il valore os pari a 1, controllo se
        // tentative_g_score e' minore del valore di g_score del nodo vicino (gY), se lo e' aggiorno le componenti
        // g_score, f_score e predessore come nel caso precedente, altrimenti le lascio invariate
        val updateNodes: RDD[((String,String,Double,Double), (Double,Double,Double, (String,String,Double,Double),
          Int, Int, ((String,String,Double,Double), Double), (String,String,Double,Double)))] = neighbours
          .map {
            case (yId, ((sourceId, weight), (gy, hy, fy, py, 0, cs, (kMin, gMin), dest))) => {
              val h = getDistanceFromLatLonInKm(yId._3, yId._4, dest._3, dest._4)
              (yId, (gMin + weight, h, gMin + weight + h, sourceId, 1, cs, (kMin, gMin), dest))
            }
            case (yId, ((sourceId, weight), (gy, hy, fy, py, 1, cs, (kMin, gMin), dest))) =>
              if(gMin + weight < gy) {
                val h = getDistanceFromLatLonInKm(yId._3, yId._4, dest._3, dest._4)
                (yId, (gMin + weight, h, gMin + weight + h, sourceId, 1, cs, (kMin, gMin), dest))
              }
              else {
                (yId, (gy, hy, fy, py, 1, cs, (kMin, gMin), dest))
              }
          }

        //una volta terminata l'analisi dei nodi vicini di x effettuata al passo precedente, l'RDD updateNodes
        // che li contiene viene unita a nodes accorpando con la reduceByKey i valori con la stessa chiave, prendendo
        // soltanto quelli che hanno l'f_score minore.
        //Di seguito aggiorno l'insieme openSet con i nuovi nodi ottenuti dalle analisi precedenti
        nodes = nodes.union(updateNodes).reduceByKey((a,b) => if(a._3 < b._3) a else b).repartition(4).cache()
        nodes.checkpoint()
        openSet = nodes.filter(a => a._2._5 != 0).repartition(4).cache()

      }
    }

    (finish,nodes)
  }


  def checkSourceAndDestinationInt(source:Int, destination:Int, textFile: RDD[Array[String]]): Unit = {
    //verifica che la sorgente sia un nodo del grafo
    val checkSource = textFile.filter(a => a(0).toInt == source)
    if (checkSource.count() == 0) {
      println("\n\nIl nodo sorgente non è presente all'interno del grafo\n\n")
      System.exit(1)
    }
    //verifica che la destinazione sia un nodo del grafo
    val checkDestination = textFile.filter(a => a(0).toInt == destination)
    if (checkDestination.count() == 0) {
      println("\n\nIl nodo destinazione non è presente all'interno del grafo\n\n")
      System.exit(1)
    }
  }

  def camminoMinimoBFInt(sc:SparkContext,textFile:RDD[Array[String]],createHopFile:Int,source:Int,
                         destination:Int): RDD[(Int,(Int,Int))] = {

    //read input file and save content in edges: RDD[(Int, (Int, Int))] where: [(destination, (source, weight))]
    var edgesRDD: RDD[(Int, (Int, Int))] = sc.emptyRDD

    if(createHopFile == 1) {
      edgesRDD = textFile.map(a => (a(0).toInt, (a(1).toInt, 1)))
        .partitionBy(new HashPartitioner(4)).persist()
      /*
      //Because the graph is undirected, read each edge twice: first "n1->n2", then "n2->n1"
      val edgesRDD1: RDD[(Int, (Int, Int))] = textFile.map(a => (a(0).toInt, (a(1).toInt, 1)))
      val edgesRDD2: RDD[(Int, (Int, Int))] = textFile.map(a => (a(1).toInt, (a(0).toInt, 1)))
      val edgesRDD: RDD[(Int, (Int, Int))] = edgesRDD1.union(edgesRDD2)
        .partitionBy(new HashPartitioner(4)).persist()
      */
    }
    else {
      edgesRDD = textFile.map(a => (a(0).toInt,(a(1).toInt,a(2).toInt)))
        .partitionBy(new HashPartitioner(4)).persist()
      /*
      //Because the graph is undirected, read each edge twice: first "n1->n2", then "n2->n1"
      val edgesRDD1: RDD[(Int, (Int, Int))] = textFile.map(a => (a(1).toInt, (a(0).toInt, a(2).toInt)))
      val edgesRDD2: RDD[(Int, (Int, Int))] = textFile.map(a => (a(1).toInt, (a(0).toInt, a(2).toInt)))
      val edgesRDD: RDD[(Int, (Int, Int))] = edgesRDD1.union(edgesRDD2)
        .partitionBy(new HashPartitioner(4)).persist()
      */
    }
    //edgesRDD.collect().foreach(println)

    //creo un RDD[(k,v)] per ogni nodo del grafo. La chiave è l'id del nodo, il valore è la coppia
    //(distanza dalla sorgente, predecessore). Inizialmente ogni nodo ha distanza dalla sorgente pari
    //a infinito e predecessore nullo (impostato a -1); solo la sorgente ha distanza pari a 0 e
    //predecessore nullo (quindi -1)
    var nodes = edgesRDD.groupByKey().map(a =>
      if (a._1 != source)
        (a._1, (1000000000, -1))
      else
        (a._1, (0, -1))
    ).repartition(4).cache()
    //nodes.collect().foreach(println)

    for (i <- 0 until nodes.count().toInt - 1) {

      //FASE 1: MAP
      //operazione 1: edgesRDD.join(nodes)
      //  ad ogni arco del grafo associo gli attributi del nodo sorgente dell'arco
      //  ottengo ad esempio (1, ((2, 4), (1000000000, -1))) ad indicare che il nodo 1 è collegato al nodo 2 con un arco
      //  di peso 4, ha distanza 1000000000 dalla sorgente e ha predecessore nullo
      //operazione2: risultatoOperazione1.map{...}
      //  riordino l'RDD precedente in modo da avere come chiave il nodo destinazione
      //  ad esempio, riprendendo il caso precedente, ottengo (2, (1, (1000000000, -1), 4))
      //operazione3: risultatoOperazione2.join(nodes)
      //  ad ogni arco del grafo associo gli attributi del nodo destinazione dell'arco
      //  riprendendo gli esempi precedenti ottengo (2, ((1, (1000000000, -1), 4), (1000000000, -1)))
      //operazione4: risultatoOperazione3.map{...}
      //  per ogni arco eseguo la computazione caratteristica di Bellman-Ford: se la distanza che la destinazione dell'
      //  arco ha dalla sorgente del grafo è maggiore della distanza che la sorgente dell'arco ha dalla sorgente del
      //  grafo sommata al peso dell'arco, allora si aggiorna il nodo destinazione dell'arco con: (distanza che la
      //  sorgente dell'arco ha dalla sorgente del grafo sommata al peso dell'arco, sorgente dell'arco). In caso
      //  contrario la destinazioe dell'arco rimane invariata. Si noti che nella computazione abbiamo invertito sorgente
      //  e destinazione dell'arco considerato, il risultato non cambia visto che il grafo è bidirezionale
      //Al termine di queste quattro operazioni ottengo un RDD[(nodo, (distanzaDallaSorgente, predecessore))]
      val updateDestination = edgesRDD.join(nodes)
        .map {
          case (s, ((d, w), as)) => (d, (s, (as._1, as._2), w))
        }
        .join(nodes)
        .map {
          case (s, ((d, ad, w), as)) =>
            if (ad._1 > as._1 + w)
              (d, (as._1 + w, s))
            else
              (d, (ad._1, ad._2))
        }
      //updateDestination.collect().foreach(println)

      //FASE 2: REDUCE
      //considero l'RDD[(nodo, (distanzaDallaSorgente, predecessore))] ottenuta al passo precedente e accorpo tutti i
      //valori con la stessa chiave prendendo quelli che hanno la distanzaDallaSorgente minore
      if (i % 20 == 0) {
        nodes = updateDestination.reduceByKey((x, y) => if (x._1 < y._1) x else y).repartition(4).cache()
        nodes.checkpoint()
      }
      else {
        nodes = updateDestination.reduceByKey((x, y) => if (x._1 < y._1) x else y)
      }
      //System.out.println("\n\n" + i + "\n\n"); nodes.collect.foreach(println)
    }
    //nodes.collect().foreach(println)

    //restituisco l'RDD con l'elenco dei nodi dove per ciascuno e' indicato:
    // - predecessore nel cammino minimo
    // - distanza dalla destinazione (numero di hop alla destinazione o peso del percorso fino alla destinazione)
    nodes

  }

  def buildPathInt(nodesMap: Map[Int, (Int, Int)], source:Int, destination:Int): Unit = {
    var path: List[Any] = List(destination)

    if (source == destination)
      println("\n\nLa destinazione coincide con la sorgente\n\n")
    else {
      //individuo il nodo destinazione memorizzando: (distanza dalla sorgente, predecessore)
      val node: (Int, Int) = nodesMap get destination match {
        case value => value.get
      }

      //memorizzo il peso del percorso dalla sorgente alla destinazione
      val weight: Int = node._1

      var i: Int = node._2
      var end: Int = 0

      do {
        //il nodo i è la sorgente: la computazione termina
        if (i == -1) {
          end = 1
        }
        //il nodo i non è la sorgente: aggiungo il predecessore di i in testa a path
        else {
          path = i :: path

          //aggiorno l'iteratore
          i = nodesMap get i match {
            case value => value.get._2
          }
        }
      } while (end == 0)

      println("\n\nPercorso da " + source + " a " + destination + " di peso " + weight + ":\n" + path.toString() + "\n\n")

    }
  }

  def checkSourceAndDestinationCities(source:(String,String),destination:(String,String),
                                      textFile:RDD[Array[String]]): Unit ={

    //verifica che la sorgente sia un nodo del grafo
    val checkS = textFile.filter(a => (a(0),a(1)) == source )
    if(checkS.count() == 0) {
      println("\n\nIl nodo sorgente non è presente all'interno del grafo\n\n")
      System.exit(1)
    }
    //verifica che la destinazione sia un nodo del grafo
    val checkD = textFile.filter(a => (a(0),a(1)) == destination)
    if(checkD.count() == 0) {
      println("\n\nIl nodo destinazione non è presente all'interno del grafo\n\n")
      System.exit(1)
    }
  }

  def camminoMinimoBFCities(sc:SparkContext,textFile:RDD[Array[String]],source:(String,String),
                            destination:(String,String)): RDD[((String,String),(Double,(String,String)))] = {

    //leggo il file in input e salvo gli archi fra le citta' nella forma: (cittaS,statoS),((citta'D,statoD), distanza)
    val edgesRDD: RDD[( (String,String),((String,String), Double))] = textFile
      .map(a => ((a(0),a(1)),((a(2),a(3)),a(4).toDouble)))
      .partitionBy(new HashPartitioner(4)).persist()
    //edgesRDD.collect().foreach(println)

    //creo un RDD[(k,v)] per ogni nodo del grafo. La chiave è l'id del nodo (citta',stato), il valore è la coppia
    //(distanza dalla sorgente, predecessore). Inizialmente ogni nodo ha distanza dalla sorgente pari
    //a infinito (1E9) e predecessore nullo (impostato a ("","")); solo la sorgente ha distanza pari a 0 e
    //predecessore nullo
    var nodes: RDD[((String,String),(Double,(String,String)))] = edgesRDD.groupByKey().map(a =>
      if (a._1 != source)
        (a._1, (1E9, ("","")))
      else
        (a._1, (0.toDouble, ("","")))
    ).repartition(4).cache()
    //nodes.collect().foreach(println)

    for (i <- 0 until nodes.count().toInt - 1) {

      //FASE 1: MAP
      //operazione 1: edgesRDD.join(nodes)
      //  ad ogni arco del grafo associo gli attributi del nodo sorgente dell'arco
      //  ottengo ad esempio ((citta1,stato1), (((citta2,stato2), 4), (1E9, ("","")))) ad indicare che la citta1 è
      //  collegata alla citta2 con distanza 4; la citta1 ha distanza infinita dalla sorgente e ha predecessore nullo
      //operazione2: risultatoOperazione1.map{...}
      //  riordino l'RDD precedente in modo da avere come chiave il nodo destinazione
      //  ad esempio, riprendendo il caso precedente, ottengo ((citta2,stato2), ((citta1,stato1), (1E9, ("","")), 4))
      //operazione3: risultatoOperazione2.join(nodes)
      //  ad ogni arco del grafo associo gli attributi del nodo destinazione dell'arco. riprendendo gli esempi
      //  precedenti ottengo ((citta2,stato2), (((citta1,stato), (1E9, ("","")), 4), (1E9, ("",""))))
      //operazione4: risultatoOperazione3.map{...}
      //  per ogni arco eseguo la computazione caratteristica di Bellman-Ford: se la distanza che la destinazione dell'
      //  arco ha dalla sorgente del grafo è maggiore della distanza che la sorgente dell'arco ha dalla sorgente del
      //  grafo sommata al peso dell'arco, allora si aggiorna il nodo destinazione dell'arco con: (distanza che la
      //  sorgente dell'arco ha dalla sorgente del grafo sommata al peso dell'arco, sorgente dell'arco). In caso
      //  contrario la destinazioe dell'arco rimane invariata. Si noti che nella computazione abbiamo invertito sorgente
      //  e destinazione dell'arco considerato, il risultato non cambia visto che il grafo è bidirezionale
      //Al termine di queste quattro operazioni ottengo un RDD[(nodo, (distanzaDallaSorgente, predecessore))]
      val updateDestination: RDD[((String,String),(Double,(String,String)))] = edgesRDD.join(nodes)
        .map {
          case (s, ((d, w), as)) => (d, (s, (as._1, as._2), w))
        }
        .join(nodes)
        .map {
          case (s, ((d, ad, w), as)) =>
            if (ad._1 > as._1 + w)
              (d, (as._1 + w, s))
            else
              (d, (ad._1, ad._2))
        }
      //println("\narchi"); updateDestination.collect().foreach(println); println("\n")

      //FASE 2: REDUCE
      //considero l'RDD[(nodo, (distanzaDallaSorgente, predecessore))] ottenuta al passo precedente e accorpo tutti i
      //valori con la stessa chiave prendendo quelli che hanno la distanzaDallaSorgente minore
      if (i % 20 == 0) {
        nodes = updateDestination.reduceByKey((x, y) => if (x._1 < y._1) x else y).repartition(4).cache()
        nodes.checkpoint()
      }
      else {
        nodes = updateDestination.reduceByKey((x, y) => if (x._1 < y._1) x else y)
      }
      //System.out.println("\n\n" + i + "\n\n"); nodes.collect.foreach(println)
    }
    //nodes.collect().foreach(println)

    //restituisco l'RDD con l'elenco dei nodi dove per ciascuno e' indicato:
    // - predecessore nel cammino minimo
    // - distanza dalla destinazione (numero di hop alla destinazione o peso del percorso fino alla destinazione)
    nodes

  }

  def buildPathCities(nodesMap:Map[(String,String),(Double,(String,String))],source:(String,String),
                      destination:(String,String)): Unit = {

    var path: List[Any] = List(destination)

    if (source == destination)
      println("\n\nIl nodo destinazione e il nodo sorgente sono equivalenti\n\n")
    else {
      //individuo il nodo destinazione memorizzando (distanza dalla sorgente, predecessore)
      val node: (Double, (String,String)) = nodesMap get destination match {
        case value => value.get
      }
      //memorizzo il peso del percorso dalla sorgente alla destinazione
      val weight = node._1
      //variabili per eseguire l'iterazione
      var i = node._2
      var end = 0

      do {
        //il nodo i è la sorgente: la computazione termina
        if (i == ("","")) end = 1
        //il nodo i non è la sorgente: aggiungo il predecessore di i in testa a path
        else {
          path = i :: path
          //aggiorno l'iteratore
          i = nodesMap get i match {
            case value => value.get._2
          }
        }
      } while (end == 0)

      println("\n\nPercorso da " + source._1 + " (" + source._2 + ") a " + destination._1 + "(" + destination._2 +
        ") di peso " + weight + ":\n" + path.toString() + "\n\n")

    }
  }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000 + "ms")
    result
  }

  def == (c1:(String,String), c2:(String,String)): Boolean = {
    if(c1._1 == c2._1 && c1._2 == c2._2) true
    else false
  }

  def != (c1:(String,String), c2:(String,String)): Boolean = {
    if(c1._1 == c2._1 && c1._2 == c2._2) false
    else true
  }

  //https://stackoverflow.com/questions/27928/calculate-distance-between-two-latitude-longitude-points-haversine-formula
  def getDistanceFromLatLonInKm(lat1: Double,lon1:Double,lat2:Double,lon2:Double): Double = {
    // Radius of the earth in km
    val R: Int = 6371

    // deg2rad below
    val dLat: Double = deg2rad(lat2 - lat1)
    val dLon: Double = deg2rad(lon2 - lon1)

    val a = Math.sin(dLat/2) * Math.sin(dLat/2) +
      Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.sin(dLon/2) * Math.sin(dLon/2)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a))

    // Distance in km
    R * c
  }

  def deg2rad(deg: Double): Double = {
    deg * (Math.PI/180)
  }

}
