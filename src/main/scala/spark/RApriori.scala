package spark

import bloomfilter.mutable.BloomFilter
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import sequential.Apriori.Itemset

import scala.collection.mutable

class RApriori extends YAFIMHashTree {

  def findPairsBloomFilter(transactions: RDD[Itemset], singletons: List[String], minSupport: Int, sc: SparkContext): List[Itemset] = {
    if (singletons.nonEmpty) {
      val bf = BloomFilter[String](singletons.size, 0.01)
      singletons.foreach(bf.add)
      val bfBC = sc.broadcast(bf)

      transactions.map(t => t.filter(bfBC.value.mightContain(_)))
        .flatMap(_.combinations(2))
        .map((_, 1))
        .reduceByKey(_ + _)
        .filter(_._2 >= minSupport)
        .map(_._1.sorted)
        .collect().toList
    }
    else List.empty[Itemset]
  }

  override def findFrequentItemsets(transactions: RDD[Itemset], singletons: RDD[(String, Int)], minSupport: Int,
                                    spark: SparkSession, sc: SparkContext): List[Itemset] = {

    val frequentItemsets = mutable.Map(1 -> singletons.map(_._1).map(List(_)).collect().toList)
    println(s"Number of singletons: ${frequentItemsets(1).size}")
    var k = 1
    while (frequentItemsets.get(k).nonEmpty) {
      k += 1

      var kFrequentItemsets = List.empty[Itemset]
      if (k == 2) {
        kFrequentItemsets = findPairsBloomFilter(transactions, frequentItemsets(1).flatten, minSupport, sc)
      }
      else {
        val candidates = candidateGeneration(frequentItemsets(k - 1), sc)
        kFrequentItemsets = filterFrequentItemsets(candidates, transactions, minSupport, sc)
      }

      if (kFrequentItemsets.nonEmpty) {
        frequentItemsets.update(k, kFrequentItemsets)
        println(s"Number of itemsets with size $k: ${frequentItemsets(k).size}")
      }
    }
    frequentItemsets.values.flatten.toList
  }

}
