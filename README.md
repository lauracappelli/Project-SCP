# Project-SCP
Progetto Salable &amp; Cloud Programming - Alma Mater Studiorum Università di Bologna - A.A. 2018-2019

### Algoritmi in scala-spark per il calcolo del cammino minimo di un grafo
Il progetto si compone di tre algotimi:
  * costruzione di un grafo bidirezionale connesso i cui nodi rappresentano i luoghi del mondo e i cui archi simulano i collegamenti
  fra tali luoghi. Il codice si trova nel file BuildConnectedCitisiesGraph.scala e richiama funzioni contenute nella libreria
  FunctionCM.scala implementata per questo progetto.
  * Algoritmo di Bellman-Ford per il calcolo del cammino minimo di un grafo. Le variabili di questo codice sono: il grafo di partenza,
  il nodo sorgente e il nodo destinazione (anche se l'algoritmo calcola il cammino minimo dalla sorgente ad ogni altro nodo del grafo).
  Il grafo in input è un file di testo contente una riga per ogni arco. I nodi possono avere due formati distinti:
    * numeri interi --> gli archi sono della forma:
    
      id_nodo_1 \t id_nodo_2 \t peso \n
    * luoghi del mondo --> gli archi sono ottenuti come output del primo algritmo e sono della forma: 
    
      ((luogo1,stato1),(luogo2,stato2,latitudine_luogo1,longitudine_luogo1,latitudine_luogo2,longitudine_luogo2,distanza_luoghi))
  
    Il codice si trova nel file BellmanFord.scala e nel file con le funzioni di libreria FunctionCM.scala.
  * Algortimo A* per il calcolo del cammino minimo di un grafo. Le variabili di questo codice sono: il grafo di partenza,
  il nodo sorgente e il nodo destinazione. Il grafo in input ha le stesse caratteristiche descritte per l'algoritmo precedente.
  Il codice si trova nel file Astar.scala e nel file con le funzioni di libreria FunctionCM.scala.
 
Il database utilizzato per questo progetto è stato scaricato dal sito https://www.geonames.org/

Gli algoritmi sono stati eseguiti sia in locale, sia su cluster forniti da Amazon Web Services con licenza
Educate (https://aws.amazon.com/it/education/awseducate/). 
