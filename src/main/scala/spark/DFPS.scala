package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import sequential.Apriori.Itemset
import sequential.FIM
import sequential.Util.absoluteSupport
import sequential.fpgrowth.{FPGrowth, FPNode, FPTree}

import scala.collection.mutable

// TODO: SparkFIM which passes transactionRDD, singletons, support and times execution
class DFPS extends FIM with Serializable {

  def dfps(transactionsRDD: RDD[Itemset], singletons: Seq[String], minSupport: Int, spark: SparkSession) = {
    transactionsRDD
      .map(t => pruneAndSort(t, singletons))
      .flatMap(buildConditionalPatternsBase)
      .groupByKey(singletons.size - 1)
      .flatMap(t => minePatternFragment(t._1, t._2.toList, minSupport))
      .collect().toList ++ singletons.map(List(_))
  }

  def minePatternFragment(prefix: String, conditionalPatterns: List[Itemset], minSupport: Int) = {
    val fpGrowth = new FPGrowth
    val singletons = mutable.LinkedHashMap(fpGrowth.findSingletons(conditionalPatterns, minSupport).map(i => i -> Option.empty[FPNode]): _*)
    val condFPTree = new FPTree(conditionalPatterns, minSupport, singletons)
    val prefixes = fpGrowth.generatePrefixes(List(prefix), singletons.keySet)

    prefixes.flatMap(p => fpGrowth.findFrequentItemsets(condFPTree, p, minSupport))
  }

  /**
    * in: f,c,a,m,p
    * out:
    * p -> f,c,a,m
    * m -> f,c,a
    * a -> f,c
    * c -> f
    */
  def buildConditionalPatternsBase(transaction: Itemset): List[(String, Itemset)] = {
    (1 until transaction.size).map(i => (transaction(i), transaction.slice(0, i))).toList
  }

  def pruneAndSort(transaction: Itemset, singletons: Seq[String]): Itemset = {
    transaction
      .filter(i => singletons.contains(i))
      .sortWith((a, b) => singletons.indexOf(a) < singletons.indexOf(b))
  }

  override def findFrequentItemsets(fileName: String, separator: String, transactions: List[Itemset], minSupport: Double): List[Itemset] = {
    val spark = SparkSession.builder()
      .appName("DFPS")
      .master("local[4]")
      //.config("spark.eventLog.enabled", "true")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    val t0 = System.currentTimeMillis()

    var transactionsRDD: RDD[Itemset] = null
    var support: Int = 0

    if (!fileName.isEmpty) {
      // Fetch transaction
      val file = getClass.getResource(fileName).getPath
      transactionsRDD = sc.textFile(file, 8)
        .filter(!_.trim.isEmpty)
        .map(_.split(separator + "+"))
        .map(l => l.map(_.trim).toList)
        .cache()
      support = absoluteSupport(minSupport, transactionsRDD.count().toInt)
    }
    else {
      transactionsRDD = sc.parallelize(transactions)
      support = absoluteSupport(minSupport, transactions.size)
    }

    // Generate singletons
    val singletons = transactionsRDD
      .flatMap(identity)
      .map(item => (item, 1))
      .reduceByKey(_ + _)
      .filter(_._2 >= support)
      .collect.toSeq
      .sortBy(_._2) // todo: sort by in spark
      .reverse.map(t => t._1)

    val result = dfps(transactionsRDD, singletons, support, spark)
    executionTime = System.currentTimeMillis() - t0
    result
  }

}
