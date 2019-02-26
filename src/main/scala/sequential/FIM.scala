package sequential

import sequential.Apriori.Itemset

trait FIM {

  def findFrequentItemsets(transactions: List[Itemset], minSupport: Int): List[Itemset]

  def findFrequentItemsets(transactions: String, minSupport: Int): List[Itemset] = {
    findFrequentItemsets(Util.parseTransactionsByText(transactions), minSupport)
  }

}
