package sequential.hashtree

import sequential.Apriori
import sequential.Apriori.Itemset
import sequential.Util.printItemsets


object AprioriHashTree {

  def main(args: Array[String]): Unit = {
    val itemsets =
      """|a,b,c
         |a,b,c
         |a,b,c
      """.stripMargin

    val frequentSets = new AprioriHashTree().execute("/datasets/chess.txt", " ", 0.85)
    printItemsets(frequentSets)
  }

}

class AprioriHashTree extends Apriori {

  override def filterFrequentItemsets(possibleItemsets: List[Itemset], transactions: List[Itemset], minSupport: Int): List[Itemset] = {
    if (possibleItemsets.nonEmpty) {
      val items = possibleItemsets.flatten.distinct
      val hashTree = new HashTree(possibleItemsets, items)

      val t1 = System.currentTimeMillis()
      val r = transactions.flatMap(t => hashTree.findCandidatesForTransaction(t.filter(i => items.contains(i)).sorted))
      println(s"Searched tree in ${(System.currentTimeMillis() - t1) / 1000}")

      val t2 = System.currentTimeMillis()
      val r2 = r.groupBy(identity)
        .map(t => (t._1, t._2.size))
        .filter(_._2 >= minSupport)
        .keys.toList
      println(s"Grouped an filtered in ${(System.currentTimeMillis() - t2) / 1000}\n")
      r2
    }
    else
      List.empty[Itemset]
  }

}
