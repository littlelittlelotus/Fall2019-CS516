import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

object NoInOutLink {
    def computeOutLinks(line: String): String = {
    line.split(":").head
    }
    def computeInlinks(line: String): String = {
    line.split(":")(1)
    }
	
    def main(args: Array[String]) {
        val input_dir = "sample_input"
        val links_file = input_dir + "/links-simple-sorted.txt"
        val titles_file = input_dir + "/titles-sorted.txt"
        val num_partitions = 10
        

        val conf = new SparkConf()
            .setAppName("NoInOutLink")
            .setMaster("local[*]")
            .set("spark.driver.memory", "1g")
            .set("spark.executor.memory", "2g")

        val sc = new SparkContext(conf)

        val links = sc
            .textFile(links_file, num_partitions)
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
        //outlinksCountsRDD.foreach(println)
        //inlinksCountsRDD.foreach(println)


        val titles = sc
            .textFile(titles_file, num_partitions)
            // TODO
	val titlesRDD = titles
            .zipWithIndex()
            .map{ case (line, i) => ((i+1).toInt, line) }



        /* No Outlinks */
	// TODO
	println("[ NO OUTLINKS ]")
        val no_outlinks = titlesRDD.subtractByKey(outlinksCountsRDD)
	no_outlinks.sortByKey().take(10).foreach(println)
        

        /* No Inlinks */
        println("\n[ NO INLINKS ]")
        // TODO
	val no_inlinks = titlesRDD.subtractByKey(inlinksCountsRDD)
	no_inlinks.sortByKey().take(10).foreach(println)
    }
}
