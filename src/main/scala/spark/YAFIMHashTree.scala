package spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import sequential.Apriori.Itemset
import sequential.hashtree.HashTree

// TODO: cache pruned transactionsRDDs?
class YAFIMHashTree extends YAFIM {

  override def filterFrequentItemsets(candidates: List[Itemset], transactionsRDD: RDD[Itemset], minSupport: Int, sc: SparkContext) = {
    if (candidates.nonEmpty) {
      val items = candidates.flatten.distinct // todo: actually helps, or should just use singletions?
      val hashTree = new HashTree(candidates, items)
      sc.broadcast(hashTree)

      val t1 = System.currentTimeMillis()
      val r = transactionsRDD.flatMap(t => hashTree.findCandidatesForTransaction(t.filter(i => items.contains(i)).sorted))
        .map(candidate => (candidate, 1))
        .reduceByKey(_ + _)
        .filter(_._2 >= minSupport)
        .map(_._1)
        .collect().toList
      println(s"Searched tree in ${(System.currentTimeMillis() - t1) / 1000}.")
      r
    }
    else List.empty[Itemset]
  }

}
