package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import sequential.Apriori.Itemset
import sequential.Util.printItemsets
import sequential.{Apriori, FIM, Util}


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
class YAFIM extends FIM {

  override def findFrequentItemsets(transactions: List[Itemset], minSupport: Int): List[Itemset] = {
    val spark = SparkSession.builder().appName("YAFIM")
      .master("local[4]")
      .getOrCreate()

    val transactionsRDD: RDD[Itemset] = spark.sparkContext.parallelize(transactions)

    val singletonsRDD = transactionsRDD
      .flatMap(identity)
      .map(item => (item, 1))
      .reduceByKey(_ + _)
      .filter(_._2 >= minSupport)
      .map(_._1)

    val candidates = new Apriori().findKItemsets(singletonsRDD.map(List(_)).collect().toList)

    // TODO:
    //  1. Implement Ct = subset(Ck, t)
    //  2. Piece YAFIM together
    //  3. Understand Hash Tree for sup counting. Why Hash Tree instead of a normal Tree?
    val prunedCandidatesRDD = transactionsRDD.flatMap(t => {
      // output all candidates from this transaction of size n. e.g:
      //    candidates: {1,3}, {1,2}, {1,4}, {5,6}
      //    t: {1,2,3}
      //    out: {1,3}, {1,2}
      // either 1. loop over all candidates and output those who are a subset of the transaction
      // manter opção 1 também
      // or, 2. generate all possible candidates of size n from T and check on the hash tree if exists and outputs it
      // 2 seems to be recommended.
      candidates.flatMap(c => {
        // candidate exists within the transaction
        if (c.intersect(t).length == c.length)
          List(c)
        else
          List.empty[Itemset]
      })
    })

    val frequentItemsets = prunedCandidatesRDD.map((_, 1))
      .reduceByKey(_ + _)
      .filter(_._2 >= minSupport)
      .map(_._1)

    frequentItemsets.collect().toList
  }

}
