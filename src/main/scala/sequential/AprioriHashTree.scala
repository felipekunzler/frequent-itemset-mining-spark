package sequential
import sequential.Apriori.Itemset
import sequential.Util.{percentageSupport, printItemsets}


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
    // todo: hash map probably faster for counting
    // todo: error with grocery and others
    val hashTree = new HashTree(possibleItemsets)
    transactions.flatMap(t => hashTree.findCandidatesForTransaction(t))
      .groupBy(identity)
      .map(t => (t._1, t._2.size))
      .filter(_._2 >= minSupport)
      .keys.toList
  }

}
