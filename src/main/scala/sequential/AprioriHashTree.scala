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
    if (possibleItemsets.nonEmpty) {
      val t0 = System.currentTimeMillis()
      val hashTree = new HashTree(possibleItemsets)
      println(s"Built tree of size ${possibleItemsets.head.size} and rows ${possibleItemsets.size} in ${(System.currentTimeMillis() - t0) / 1000}")
      val t1 = System.currentTimeMillis()
      val subsets = transactions.flatMap(t => t.sorted.combinations(possibleItemsets.head.size))
      println(s"Generated ${subsets.size} subsets in ${(System.currentTimeMillis() - t1) / 1000}")

      val t2 = System.currentTimeMillis()
      subsets.foreach(s => hashTree.incrementCandidatesForSubset(s))
      println(s"Searched tree in ${(System.currentTimeMillis() - t2) / 1000}\n")

      val r = hashTree.findFrequents(minSupport)
      r.toList
    }
    else
      List.empty[Itemset]
  }

}
