# Frequent Itemset Mining in Scala and Spark
Implementation of Apriori and FP-Growth in Scala, as well as their counterpart distributed implementations YAFIM, RApriori and DFPS using Spark.

This work has been developed as part of my [CS undergraduate thesis](./Final%20Paper%20-%20pt_BR.pdf) where three previously proposed distributed algorithms were implemented in Spark and compared on [Amazon EMR](https://aws.amazon.com/emr/).

### Algorithms
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

### Experiments
The three distributed FIM algorithms were compared in terms of execution time and normalized speedup.
All experiments were run on Amazon EMR using m5.xlarge (dual core) machines.

* Execution time in seconds. Datasets replicated 9 times and increasing cluster size.

<kbd><img src="https://user-images.githubusercontent.com/9336586/58119272-f845ff00-7bd8-11e9-8c78-86304556fe52.png" height="275" width="400"></kbd> <kbd><img src="https://user-images.githubusercontent.com/9336586/58119293-03009400-7bd9-11e9-9724-0dcbed6b3b00.png" height="275" width="400"></kbd>
<kbd><img src="https://user-images.githubusercontent.com/9336586/58120009-b027dc00-7bda-11e9-9da9-e09b20670c0b.png" height="275" width="400"></kbd> <kbd><img src="https://user-images.githubusercontent.com/9336586/58119984-9e463900-7bda-11e9-8bec-c9a012effb42.png" height="275" width="400"></kbd>

* Normalized speedup factor, if the number of cores is increased to 10, the ideal speedup factor is 10 (meaning that the execution time has decreased by 10 times). Datasets replicated 9 times and increasing cluster size (X axis measured in cores).

<kbd><img src="https://user-images.githubusercontent.com/9336586/58120022-ba49da80-7bda-11e9-81b9-14d827e246bf.png" height="275" width="400"></kbd> <kbd><img src="https://user-images.githubusercontent.com/9336586/58120036-c766c980-7bda-11e9-926c-9e2c5792e4c0.png" height="275" width="400"></kbd>
<kbd><img src="https://user-images.githubusercontent.com/9336586/58120028-c0d85200-7bda-11e9-8c25-f6891c13d047.png" height="275" width="400"></kbd> <kbd><img src="https://user-images.githubusercontent.com/9336586/58120051-cdf54100-7bda-11e9-8ff1-11fe543f3093.png" height="275" width="400"></kbd>

### Building and running
* Install and set Java 8 to your path, e.g. `sdk install java 8.0.252.hs-adpt` and `sdk use java 8.0.252.hs-adpt`
* Instsall sbt, e.g. `sdk install sbt`
* Compile the project using `sbt compile`
* Adjust the absolute path for the provided datasets in `src/main/resources/defaultfim.properties`
* Run the experiments with `sbt run` and select the main class `experiments.Runner`

Sample output (execution times in seconds):
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

Additionally, algorithms can be run individually by instantiating its respective class. Example:
```scala
object Main {
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
