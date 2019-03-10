package sequential.fpgrowth

import sequential.Apriori.Itemset
import sequential.FIM
import sequential.Util.printItemsets

object FPGrowth {

  def main(args: Array[String]): Unit = {
    val itemsets =
      """|1,3
         |1,2,3
         |1
         |1,3,4
         |3
         |1,2
         |1,2,3,4
      """.stripMargin
    val frequentItemsets = new FPGrowth().execute(itemsets, 1)
    printItemsets(frequentItemsets)
  }

}

/**
  * 1. Find singletons, order by most frequent
  * 2. Build FP-Tree by creating all possible paths (TODO: Links between items)
  * 3. Build conditional FP-Tree
  */
class FPGrowth extends FIM {

  override def findFrequentItemsets(transactions: List[Itemset], minSupport: Int): List[Itemset] = {
    val singletons = findSingletons(transactions, minSupport)
    val fpTree = new FPTree(transactions, minSupport, singletons)
    fpTree.rootNode


    List()
  }

  private def findSingletons(transactions: List[Itemset], minSupport: Int) = {
    transactions.flatten
      .groupBy(identity)
      .map(t => (t._1, t._2.size))
      .filter(_._2 >= minSupport)
      .toSeq.sortBy(_._2)
      .reverse.map(t => t._1)
  }

}
