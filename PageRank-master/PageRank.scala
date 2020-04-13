import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

object PageRank {

  def main(args: Array[String]) {
    
    val inputDir = "sample-input"
    val linksFile = inputDir + "/links-simple-sorted.txt"
    val titlesFile = inputDir + "/titles-sorted.txt"
    val numPartitions = 10
    val numIters = 10
    var damping = 0.85

    val conf = new SparkConf()
      .setAppName("PageRank")
      .setMaster("local[*]")
      .set("spark.driver.memory", "1g")
      .set("spark.executor.memory", "2g")

    val sc = new SparkContext(conf)

    val links = sc
      .textFile(linksFile, numPartitions)
    // TODO
    val outlinksRDD = links.map{ link =>
      val key = link.split(":").head.toInt
      val values = link.split(":")(1).split("\\s+").tail.map(_.toInt)
        (key, values) //(key, values.toList.toString)
    }
    //println("check outlinksRDD")
    //outlinksRDD.foreach(println)

    val titles = sc
      .textFile(titlesFile, numPartitions)
    // TODO
    val titlesRDD = titles
      .zipWithIndex()
      .map{ case (line, i) => ((i+1).toInt, line) }
    // println("check titles")
    // titlesRDD.foreach(println)
    
    val N = titles.count
    var ranksRDD = titlesRDD
      .mapValues(title => 100.0 / N)
    //println("check PR_0:")
    //ranksRDD.foreach(println)
    var ranksRDD1 = ranksRDD
    
    /* PageRank */
    for (i <- 1 to 10) {
      val contribs = ranksRDD.join(outlinksRDD)
        .values
        .flatMap{ case (rank, urls) =>
           val size = urls.size
            urls.map(url => (url, rank / size))
        }

      ranksRDD1 = contribs
        .reduceByKey(_+_)
        .mapValues( ((1-damping)*100.0/ N) + damping*_ ) 

      //ranksRDD = ranksRDD1
      // println(f"check updated PR_i iter $i%d:")
      // ranksRDD1.foreach(println)

      
      val ranksRDD2 = ranksRDD
       .subtractByKey(ranksRDD1)
       .mapValues(x => 0.15 * 100.0 / N)
      
      ranksRDD = ranksRDD1 ++ ranksRDD2
      
      //println("check joins")
      //ranksRDD.foreach(println)
    }
    /*
    val ranksRDD2 = ranksRDD
        .subtractByKey(ranksRDD1)
        .mapValues(x => 0.15 * 100.0 / N)
      
    ranksRDD = ranksRDD1 ++ ranksRDD2
    */
    println("[ PageRanks ]")
    // TODO
    // normalization: 
    val ranksum = ranksRDD.map(_._2).sum()
    val finalranks = ranksRDD.mapValues(x => x * 100 / ranksum)
    val finalRDD = titlesRDD.join(finalranks)
      .map { case (index, (url, rankval)) => (index, url, rankval)
      }
    finalRDD.sortBy(_._3, false).take(10).foreach(println)  
  }
}
