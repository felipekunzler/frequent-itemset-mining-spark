package spark

import experiments.Runner
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import sequential.Apriori.Itemset
import sequential.{FIM, Util}
import sequential.Util.absoluteSupport


trait SparkFIM extends FIM {

  /**
    * Implemented by YAFIM, RApriori or DFPS
    */
  def findFrequentItemsets(transactions: RDD[Itemset], singletons: RDD[(String, Int)], minSupport: Int,
                           spark: SparkSession, sc: SparkContext): List[Itemset]

  /**
    * Common method for all Spark FIM implementations.
    * Generates a transaction and singletons RDD as well as calculate minimum support from a percentage.
    */
  override def findFrequentItemsets(fileName: String, separator: String, transactions: List[Itemset], minSupport: Double): List[Itemset] = {
    var spark: SparkSession = null
    val appName = Util.appName
    if (!Runner.clusterMode) {
      spark = SparkSession.builder()
        .appName(appName)
        .master("local[4]")
        //.config("spark.eventLog.enabled", "true")
        .getOrCreate()
    }
    else {
      spark = SparkSession.builder().appName(appName).getOrCreate()
    }

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    val t0 = System.currentTimeMillis()

    var transactionsRDD: RDD[Itemset] = null
    var support: Int = 0

    if (!fileName.isEmpty) {
      // Fetch transaction
      val file = List.fill(Util.replicateNTimes)(fileName).mkString(",")
      var fileRDD: RDD[String] = null
      if (Util.minPartitions == -1)
        fileRDD = sc.textFile(file)
      else
        fileRDD = sc.textFile(file, Util.minPartitions)

      transactionsRDD = fileRDD.filter(!_.trim.isEmpty)
        .map(_.split(separator + "+"))
        .map(l => l.map(_.trim).toList)

      if (Util.props.getProperty("fim.cache", "false").toBoolean) {
        transactionsRDD = transactionsRDD.cache()
        println("cached")
      }
      support = absoluteSupport(minSupport, transactionsRDD.count().toInt)
    }
    else {
      transactionsRDD = sc.parallelize(transactions)
      support = absoluteSupport(minSupport, transactions.size)
    }

    // Generate singletons
    val singletonsRDD = transactionsRDD
      .flatMap(identity)
      .map(item => (item, 1))
      .reduceByKey(_ + _)
      .filter(_._2 >= support)

    val frequentItemsets = findFrequentItemsets(transactionsRDD, singletonsRDD, support, spark, sc)

    executionTime = System.currentTimeMillis() - t0

    if (Util.props.getProperty("fim.unpersist", "false").toBoolean) {
      transactionsRDD.unpersist()
      println("unpersited")
    }

    if (Util.props.getProperty("fim.closeContext", "false").toBoolean) {
      spark.sparkContext.stop()
      println("stopped")
    }

    frequentItemsets
  }

}
