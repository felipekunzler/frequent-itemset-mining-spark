package spark

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import sequential.Apriori.Itemset
import sequential.Util.absoluteSupport
import sequential.{Apriori, FIM}

import scala.collection.mutable


object YAFIM {

  def main(args: Array[String]): Unit = {
    val fim = new YAFIM
    val frequentSets = fim.execute("/datasets/mushroom.txt", " ", 0.35)
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
class YAFIM extends FIM with Serializable {

  override def findFrequentItemsets(fileName: String, separator: String, transactions: List[Itemset], minSupport: Double): List[Itemset] = {
    val spark = SparkSession.builder()
      .appName("YAFIM")
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
      //val file = List.fill(4)(getClass.getResource(fileName).getPath).mkString(",")
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

      // Candidate generation and initial pruning
      val candidates = candidateGeneration(frequentItemsets(k - 1), sc)
      val candidatesBC = sc.broadcast(candidates)

      // Final filter by checking with all transactions
      val kFrequentItemsetsRDD = filterFrequentItemsets(candidatesBC, transactionsRDD, support)
      if (!kFrequentItemsetsRDD.isEmpty()) {
        frequentItemsets.update(k, kFrequentItemsetsRDD.collect().toList)
      }
    }
    val result = frequentItemsets.values.flatten.toList
    executionTime = System.currentTimeMillis() - t0
    result
  }

  private def candidateGeneration(frequentSets: List[Itemset], sc: SparkContext) = {
    val apriori = new Apriori with Serializable

    val previousFrequentSets = sc.parallelize(frequentSets)
    val cartesian = previousFrequentSets.cartesian(previousFrequentSets)
      .filter { case (a, b) =>
        a.mkString("") > b.mkString("")
      }
    cartesian
      .flatMap({ case (a, b) =>
        var result: List[Itemset] = null
        if (a.size == 1 || apriori.allElementsEqualButLast(a, b)) {
          val newItemset = (a :+ b.last).sorted
          if (apriori.isItemsetValid(newItemset, frequentSets))
            result = List(newItemset)
        }
        if (result == null) List.empty[Itemset] else result
      })
      .collect().toList
  }

  private def filterFrequentItemsets(candidates: Broadcast[List[Itemset]], transactionsRDD: RDD[Itemset], minSupport: Int) = {
    val apriori = new Apriori with Serializable
    val filteredCandidatesRDD = transactionsRDD.flatMap(t => {
      candidates.value.flatMap(c => {
        // candidate exists within the transaction
        //if (c.intersect(t).length == c.length)
        if (apriori.candidateExistsInTransaction(c, t))
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
