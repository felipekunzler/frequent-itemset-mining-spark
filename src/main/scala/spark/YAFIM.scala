package spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import sequential.Apriori
import sequential.Apriori.Itemset

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
class YAFIM extends SparkFIM with Serializable {

  override def findFrequentItemsets(transactions: RDD[Itemset], singletons: RDD[(String, Int)], minSupport: Int,
                                    spark: SparkSession, sc: SparkContext): List[Itemset] = {

    val frequentItemsets = mutable.Map(1 -> singletons.map(_._1).map(List(_)).collect().toList)
    var k = 1
    while (frequentItemsets.get(k).nonEmpty) {
      k += 1

      // Candidate generation and initial pruning
      val candidates = candidateGeneration(frequentItemsets(k - 1), sc)

      // Final filter by checking with all transactions
      val kFrequentItemsetsRDD = filterFrequentItemsets(candidates, transactions, minSupport, sc)
      if (!kFrequentItemsetsRDD.isEmpty()) {
        frequentItemsets.update(k, kFrequentItemsetsRDD.collect().toList)
      }
    }
    frequentItemsets.values.flatten.toList
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

  def filterFrequentItemsets(candidates: List[Itemset], transactionsRDD: RDD[Itemset], minSupport: Int, sc: SparkContext) = {
    val apriori = new Apriori with Serializable
    val candidatesBC = sc.broadcast(candidates)
    val filteredCandidatesRDD = transactionsRDD.flatMap(t => {
      candidatesBC.value.flatMap(c => {
        // candidate exists within the transaction
        //if (t.intersect(itemset).size == itemset.size) { TODO: Why intersect so much slower?
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
