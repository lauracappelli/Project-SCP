import java.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.commons.io.FileUtils
import org.apache.spark.HashPartitioner

object PageRank {

  def main(args: Array[String]) {
    //Create a SparkContext to initialize Spark
    val conf = new SparkConf().setAppName("pageRank").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //set output folder and input file
    val inputFile = "src/main/resources/gww.txt"
    val outputFile = "src/main/resources/ResultsGraph"

    val input = sc.textFile(inputFile)
    val edges = input.map(s => s.split("\t")).map(a => (a(0).toInt,a(1).toInt))

    val links = edges.groupByKey().partitionBy(new HashPartitioner(4)).persist()
    println("links")
    //links.collect().foreach(println)

    var ranks = links.mapValues(v => 1.0)
    println("ranks")
    //ranks.collect().foreach(println)

    /*val c = links.join(ranks)
    println("c")
    c.collect().foreach(println)*/

    for(i <- 0 until 10) {
      val contributions = links.join(ranks).flatMap {
        case (u, (uLinks, rank)) => uLinks.map(t => (t, rank / uLinks.size))
      }
      //println("cont")
      //contributions.collect().foreach(println)

      ranks = contributions.reduceByKey((x,y) => x+y).mapValues(v => 0.15+0.85*v)
    }

    //save results and stop spark context
    FileUtils.deleteDirectory(new File(outputFile))
    ranks.saveAsTextFile(outputFile)
    sc.stop()
  }
}


