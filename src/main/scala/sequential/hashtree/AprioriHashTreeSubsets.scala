package sequential.hashtree

import sequential.Apriori
import sequential.Apriori.Itemset


class AprioriHashTreeSubsets extends Apriori {

  override def filterFrequentItemsets(possibleItemsets: List[Itemset], transactions: List[Itemset], minSupport: Int): List[Itemset] = {
    if (possibleItemsets.nonEmpty) {
      val t0 = System.currentTimeMillis()
      val hashTree = new HashTreeSubsets(possibleItemsets)
      println(s"Built tree of size ${possibleItemsets.head.size} and rows ${possibleItemsets.size} in ${(System.currentTimeMillis() - t0) / 1000}")
      val t1 = System.currentTimeMillis()
      // maybe generate subsets using the latest frequent itemsets?
      val subsets = transactions.flatMap(t => t.filter(i => singletons.contains(i)).sorted.combinations(possibleItemsets.head.size))
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
