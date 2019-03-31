package spark

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import sequential.Apriori.Itemset
import sequential.Util.printItemsets
import sequential.{Apriori, FIM, Util}

import scala.collection.mutable


object YAFIM {

  def main(args: Array[String]): Unit = {
    val transactions = Util.parseTransactionsByText(
      """
        |1,3
        |1,2,3
        |1
        |1,3,4
        |3
        |1,2
        |1,2,3,4
      """.stripMargin)
    val frequentItemsets = new YAFIM().execute(transactions, 3)
    printItemsets(frequentItemsets)
  }

}

/**
  * YAFIM (Yet Another Frequent Itemset Mining) algorithm implementation.
  * 1. Generate singletons
  * 2. Find K+1 frequent itemsets
  */
// output all candidates from this transaction of size n. e.g:
//    candidates: {1,3}, {1,2}, {1,4}, {5,6}
//    t: {1,2,3}
//    out: {1,3}, {1,2}
// either 1. loop over all candidates and output those who are a subset of the transaction
// manter opção 1 também
// or, 2. generate all possible candidates of size n from T and check on the hash tree if exists and outputs it
// 2 seems to be recommended.
// TODO:
//  1. Implement Ct = subset(Ck, t). Candidates from transaction
//  2. Piece YAFIM together
//  3. Understand Hash Tree for sup counting. Why Hash Tree instead of a normal Tree?
class YAFIM extends FIM {

  override def findFrequentItemsets(fileName: String, separator: String, transactions: List[Itemset], minSupport: Double): List[Itemset] = {
    val spark = SparkSession.builder()
      .appName("YAFIM")
      .master("local[4]")
      .config("spark.eventLog.enabled", "true")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    val t0 = System.currentTimeMillis()

    var transactionsRDD: RDD[Itemset] = null
    var support: Int = 0
    if (!fileName.isEmpty) {
      transactionsRDD = sc.textFile(getClass.getResource(fileName).getPath)
        .filter(!_.trim.isEmpty)
        .map(_.split(separator))
        .map(l => l.map(_.trim).toList)
        .cache()
    }
    else {
      transactionsRDD = sc.parallelize(transactions)
      support = Util.absoluteSupport(minSupport, transactions.size)
    }

    val singletonsRDD = transactionsRDD
      .flatMap(identity)
      .map(item => (item, 1))
      .reduceByKey(_ + _)
      .filter(_._2 >= support)
      .map(_._1)

    val frequentItemsets = mutable.Map(1 -> singletonsRDD.map(List(_)).collect().toList)

    var k = 1
    while (frequentItemsets.get(k).nonEmpty) {
      k += 1
      val candidates = new Apriori().findKItemsets(frequentItemsets(k - 1))
      val candidatesBC = sc.broadcast(candidates)
      val kFrequentItemsetsRDD = filterFrequentItemsets(candidatesBC, transactionsRDD, support)
      if (!kFrequentItemsetsRDD.isEmpty()) {
        frequentItemsets.update(k, kFrequentItemsetsRDD.collect().toList)
      }
    }
    val result = frequentItemsets.values.flatten.toList
    executionTime = System.currentTimeMillis() - t0
    result
  }

  private def filterFrequentItemsets(candidates: Broadcast[List[Itemset]], transactionsRDD: RDD[Itemset], minSupport: Int) = {
    val filteredCandidatesRDD = transactionsRDD.flatMap(t => {
      candidates.value.flatMap(c => {
        // candidate exists within the transaction
        if (c.intersect(t).length == c.length)
          List(c)
        else
          List.empty[Itemset]
      })
    })

    filteredCandidatesRDD.map((_, 1))
      .reduceByKey(_ + _)
      .filter(_._2 >= minSupport)
      .map(_._1)
  }

}
