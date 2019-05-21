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

### Building and running
* Compile the project using `sbt compile`
* Adjust the absolute path for the provided datasets in `src/main/resources/defaultfim.properties`
* Run the experiments with `sbt run` and select the main class `experiments.Runner`

Would output the following (execution times in seconds):
```
+-----------------+------------+-------+-------+-------+-------+------+
|           Class |    Dataset | Run 1 | Run 2 | Run 3 |  Mean |   SD |
+-----------------+------------+-------+-------+-------+-------+------+
| AprioriHashTree |   mushroom | 14.90 | 13.96 | 14.65 | 14.50 | 0.40 |
|        FPGrowth |   mushroom |  0.63 |  0.55 |  0.64 |  0.61 | 0.04 |
|   YAFIMHashTree |   mushroom |  7.26 |  6.64 |  7.92 |  7.27 | 0.52 |
|        RApriori |   mushroom |  6.71 |  6.79 |  7.45 |  6.98 | 0.33 |
|            DFPS |   mushroom |  2.49 |  2.31 |  2.27 |  2.36 | 0.10 |
|                 |            |       |       |       |       |      |
| AprioriHashTree | T10I4D100K |  3.68 |  2.39 |  2.42 |  2.83 | 0.60 |
|        FPGrowth | T10I4D100K |  3.26 |  3.27 |  3.06 |  3.20 | 0.10 |
|   YAFIMHashTree | T10I4D100K |  1.96 |  1.90 |  1.89 |  1.92 | 0.03 |
|        RApriori | T10I4D100K |  1.89 |  1.80 |  1.75 |  1.81 | 0.06 |
|            DFPS | T10I4D100K |  4.13 |  4.07 |  4.14 |  4.11 | 0.03 |
+-----------------+------------+-------+-------+-------+-------+------+

+------------+-----------------+----------+---------------+----------+------+
|            | AprioriHashTree | FPGrowth | YAFIMHashTree | RApriori | DFPS |
+------------+-----------------+----------+---------------+----------+------+
|   mushroom |           14.50 |     0.61 |          7.27 |     6.98 | 2.36 |
| T10I4D100K |            2.83 |     3.20 |          1.92 |     1.81 | 4.11 |
+------------+-----------------+----------+---------------+----------+------+
```

Additionally, individual algorithms can be run individually by instantiating its respective class. Example:
```
object YAFIM {
  
  def main(args: Array[String]): Unit = {
    
    val dfps: SparkFIM = new DFPS()
    
    val fileName = "/projects/spark-scala/frequent-itemset-mining-spark/src/main/resources/datasets/mushroom.txt"
    val frequentItemsets = dfps.execute(fileName, separator = " ", minSupport = 0.65)
    
    Util.printItemsets(frequentItemsets)
    
  }
  
}
```

Would output:
```
Elapsed time: 2.06 seconds. Class: DFPS.
Found 39 itemsets
[1] {34}, {36}, {39}, {85}, {86}, {90}
[2] {34, 36}, {34, 39}, {34, 85}, {34, 86}, {34, 90}, {36, 85}, {36, 86}, {36, 90}, {39, 85}, {39, 86}, {85, 86}, {85, 90}, {86, 90}
[3] {34, 36, 85}, {34, 36, 86}, {34, 36, 90}, {34, 39, 85}, {34, 39, 86}, {34, 85, 86}, {34, 85, 90}, {34, 86, 90}, {36, 85, 86}, {36, 85, 90}, {36, 86, 90}, {39, 85, 86}, {85, 86, 90}
[4] {34, 36, 85, 86}, {34, 36, 85, 90}, {34, 36, 86, 90}, {34, 39, 85, 86}, {34, 85, 86, 90}, {36, 85, 86, 90}
[5] {34, 36, 85, 86, 90}
```