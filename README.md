# Frequent Itemset Mining in Scala and Spark
* Sequential implementation of Apriori and FP-Growth in Scala.
  * [Apriori.scala](https://github.com/felipekunzler/frequent-itemset-mining-spark/blob/master/src/main/scala/sequential/Apriori.scala)
  * [FPGrowth.scala](https://github.com/felipekunzler/frequent-itemset-mining-spark/tree/master/src/main/scala/sequential/fpgrowth/FPGrowth.scala) and [FPTree.scala](https://github.com/felipekunzler/frequent-itemset-mining-spark/tree/master/src/main/scala/sequential/fpgrowth/FPTree.scala)
  * Apriori using a Hash Tree for filtering: [AprioriHashTree.scala](https://github.com/felipekunzler/frequent-itemset-mining-spark/blob/master/src/main/scala/sequential/hashtree/AprioriHashTree.scala)
  * Hash Tree implementation: [HashTree.scala](https://github.com/felipekunzler/frequent-itemset-mining-spark/blob/master/src/main/scala/sequential/hashtree/HashTree.scala)


* Distributed implementations of Apriori and FP-Growth using Spark.
  * [YAFIM.scala](https://github.com/felipekunzler/frequent-itemset-mining-spark/blob/master/src/main/scala/spark/YAFIM.scala)
  * YAFIM using a Hash Tree for filtering: [YAFIMHashTree.scala](https://github.com/felipekunzler/frequent-itemset-mining-spark/blob/master/src/main/scala/spark/YAFIMHashTree.scala)
  * [RApriori.scala](https://github.com/felipekunzler/frequent-itemset-mining-spark/blob/master/src/main/scala/spark/RApriori.scala)
  * [DFPS.scala](https://github.com/felipekunzler/frequent-itemset-mining-spark/blob/master/src/main/scala/spark/DFPS.scala)

* Distributed algorithms originally proposed by the following papers:
  * [YAFIM: A Parallel Frequent Itemset Mining Algorithm with Spark](https://ieeexplore.ieee.org/abstract/document/6969575)
  * [R-Apriori: An Efficient Apriori based Algorithm on Spark](https://dl.acm.org/citation.cfm?id=2809893)
  * [DFPS: Distributed FP-growth algorithm based on Spark](https://ieeexplore.ieee.org/document/8054308)
