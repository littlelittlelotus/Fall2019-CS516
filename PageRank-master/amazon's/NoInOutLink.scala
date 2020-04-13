import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

object NoInOutLink {

  def main(args: Array[String]) {
    val MASTER_ADDRESS = "ec2-34-202-166-126.compute-1.amazonaws.com"
    val SPARK_MASTER = "spark://" + MASTER_ADDRESS + ":7077"
    val HDFS_MASTER = "hdfs://" + MASTER_ADDRESS + ":9000"
    val INPUT_DIR = HDFS_MASTER + "/pagerank/input"
    val OUTPUT_DIR = HDFS_MASTER + "/pagerank/output"

    //val inputDir = INPUT_DIR + "/sample-input"
    val linksFile = INPUT_DIR + "/links-simple-sorted.txt"
    val titlesFile = INPUT_DIR + "/titles-sorted.txt"
    val numPartitions = 10

    val conf = new SparkConf()
      .setAppName("NoInOutLink")
      .setMaster(SPARK_MASTER)
      .set("spark.driver.memory", "1g")
      .set("spark.executor.memory", "2g")

    val sc = new SparkContext(conf)

    val links = sc
      .textFile(linksFile, numPartitions)
    // TODO

    val outlinksRDD = links
      .map(computeOutLinks)
    val inlinksRDD = links
      .map(computeInlinks)

    val inlinkRDD = inlinksRDD
      .flatMap(line => line.split("\\s+"))
      .filter(link => link != "")
    val outlinksCountsRDD = outlinksRDD
                    .map(word => (word.toInt, 1))
                    .reduceByKey(_+_)
    val inlinksCountsRDD = inlinkRDD
                    .map(word => (word.toInt, 1))
                    .reduceByKey(_+_)
    // println(inlinks) ShuffledRDD[7] at reduceByKey at NoInOutLink.scala:32
    println("check outlinks result")
    outlinksCountsRDD.foreach(println)
    println("check inlinks result")
    inlinksCountsRDD.foreach(println)

    val titles = sc
      .textFile(titlesFile, numPartitions)
    // TODO
    // Goal: creat a map (key, value): (index, title)
    val titlesRDD = titles
      .zipWithIndex()
      .map{ case (line, i) => ((i+1).toInt, line) }
    //println("check titles result: ")
    //titlesRDD.foreach(println)

    /* No Outlinks */
    val noOutlinks = () // TODO
    println("[ NO OUTLINKS ]")
    // TODO
    val nooutLinksRDD = titlesRDD.subtractByKey(outlinksCountsRDD)
    //nooutLinksRDD.sortByKey()
    nooutLinksRDD.sortByKey().take(10).foreach(println)
    //nooutLinksRDD
      //.saveAsTextFile(OUTPUT_DIR)

    /* No Inlinks */
    val noInlinks = () // TODO
    println("\n[ NO INLINKS ]")
    // TODO
    val noinLinksRDD = titlesRDD.subtractByKey(inlinksCountsRDD)
    //noinLinksRDD.sortByKey()
    noinLinksRDD.sortByKey().take(10).foreach(println)

    //noinLinksRDD
      //.saveAsTextFile(OUTPUT_DIR)
  }

  def computeOutLinks(line: String): String = {
    line.split(":").head
  }
  def computeInlinks(line: String): String = {
    line.split(":")(1)
  }
}
