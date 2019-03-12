import java.io._
import java.io.{BufferedWriter, File, FileWriter}

import FunctionCM._
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import scala.util.Random

object CreateCitiesGraph {

  def main(args: Array[String]): Unit = {

    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("CreateCitiesGraph")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir("src/checkpoint")

    val distance = (200, 207)
    val retDim = 5

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
          (obtainRetNumber(coord._1, coord._2, retDim),(citta, stato, coord._1, coord._2))
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
      edgeInsideReticCartesian.map(el => el.map(e => computeDistance(e._1, e._2, distance, 0)))
      //elimino tutti gli archi delle citta' a distanza infinita (=1000000000 )
      .map( el => el.filter( e => e._3 != 1000000000 ))
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
            case('N', citta) => computeDistance(citta, cittaConfineRet.filter( el => el._1 == 'S').head._2, distance, 1)
            case('S', citta) => computeDistance(citta, cittaConfineRet.filter( el => el._1 == 'N').head._2, distance, 1)
            case('E', citta) => computeDistance(citta, cittaConfineRet.filter( el => el._1 == 'W').head._2, distance, 1)
            case('W', citta) => computeDistance(citta, cittaConfineRet.filter( el => el._1 == 'E').head._2, distance, 1)
          }
      }
//guarda qua
      .repartition(4).cache()
    edgeBetweenRetic.checkpoint()

    //uniamo gli archi interni di ogni reticolo con gli archi che collegano i reticoli fra loro
    val totalEdge: RDD[Iterable[((String,String, Double, Double), (String,String, Double, Double), Double)]] =
      edgeInsideRetic.union(edgeBetweenRetic)
//guarda qua
      .persist()

    /* ****************************************************************************************************************
    PARTE 6 - SCRITTURA ARCHI SU FILE
    **************************************************************************************************************** */

    //creo il bufferReader e definisco il nome del file di output
    val outF = "src/main/resources/edgeCities.txt"
    val outputFile = new File(outF)
    val bw = new BufferedWriter(new FileWriter(outputFile))

    //memorizzo sul driver tutti gli archi
    val te = totalEdge.flatMap(a => a.map(e => e)).collect()

    println("\n\nCittà rimaste dopo il controllo sulla distanza: " + te.map(a => a._1).distinct.length)
    println("Archi totali: " + te.length + "\n\n")

    //per ogni reticolo e per ogni arco, scrivo l'arco sul file
    for (elem <- te){
      bw.write(elem._1._1 + "\t" + elem._1._2 + "\t" + elem._2._1 + "\t" + elem._2._2 + "\t" +
        elem._1._3 + "\t" + elem._1._4 + "\t" + elem._2._3 + "\t" + elem._2._4 + "\t" +  elem._3 + "\n")
    }

    //save results and stop spark context
    bw.close()
    //FileUtils.deleteDirectory(new File(outputFolder))
    //db.saveAsTextFile(outputFolder)
    sc.stop()

  }
}
