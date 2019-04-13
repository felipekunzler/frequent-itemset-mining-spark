package spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import sequential.Apriori.Itemset
import sequential.fpgrowth.{FPGrowth, FPNode, FPTree}

import scala.collection.mutable

class DFPS extends SparkFIM with Serializable {

  override def findFrequentItemsets(transactions: RDD[Itemset], singletons: RDD[(String, Int)], minSupport: Int,
                                    spark: SparkSession, sc: SparkContext): List[Itemset] = {
    // Generate singletons
    val sortedSingletons = singletons.collect.map(t => t._1)

    transactions
      .map(t => pruneAndSort(t, sortedSingletons))
      .flatMap(buildConditionalPatternsBase)
      .groupByKey(sortedSingletons.length - 1)
      .flatMap(t => minePatternFragment(t._1, t._2.toList, minSupport))
      .collect().toList ++ sortedSingletons.map(List(_))
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

}
