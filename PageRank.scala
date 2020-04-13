import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

object PageRank {
    def main(args: Array[String]) {
        val MASTER_ADDRESS = "ec2-3-93-20-212.compute-1.amazonaws.com"
        val SPARK_MASTER = "spark://" + MASTER_ADDRESS + ":7077"
        val HDFS_MASTER = "hdfs://" + MASTER_ADDRESS + ":9000"
        val input_dir = HDFS_MASTER + "/sample_input"
        val links_file = input_dir + "/links-simple-sorted.txt"
        val titles_file = input_dir + "/titles-sorted.txt"
        val num_partitions = 10
        val iters = 10
	val damping = 0.85

        val conf = new SparkConf()
            .setAppName("PageRank")
            .setMaster(SPARK_MASTER)
            .set("spark.driver.memory", "1g")
            .set("spark.executor.memory", "2g")

        val sc = new SparkContext(conf)

        val links = sc
            .textFile(links_file, num_partitions)
        // TODO
	val no_outlinks = links.map{ link =>
      		val key = link.split(":").head.toInt
      		val values = link.split(":")(1).split("\\s+").tail.map(_.toInt)
        (key, values) 
    	}

        val titles = sc
            .textFile(titles_file, num_partitions)
        // TODO
	val no_titles = titles
      		.zipWithIndex()
      		.map{ case (line, i) => ((i+1).toInt, line) }

	val N = titles.count
	var no_ranks = no_titles
		.mapValues(title => 100.0 / N)

	var temp_ranks = no_ranks

        /* PageRank */
        for (i <- 1 to iters) {
            val contribs = no_ranks.join(no_outlinks)
        	.values
        	.flatMap{ case (rank, urls) =>
           		val size = urls.size
            		urls.map(url => (url, rank / size))
        }
        
	temp_ranks = contribs
        .reduceByKey(_+_)
        .mapValues( ((1-damping)*100.0/ N) + damping*_ )

	val temp2_ranks = no_ranks
       .subtractByKey(temp_ranks)
       .mapValues(x => 0.15 * 100.0 / N)
      
        no_ranks = temp_ranks ++ temp2_ranks	
	
	}
        println("[ PageRanks ]")
        // TODO
	val ranksum = no_ranks.map(_._2).sum()
    	val finalranks = no_ranks.mapValues(x => x * 100 / ranksum)
    	val finalRDD = no_titles.join(finalranks)
      .map { case (index, (url, rankval)) => (index, url, rankval)
      }
    finalRDD.sortBy(_._3, false).take(10).foreach(println)  
    }
}
