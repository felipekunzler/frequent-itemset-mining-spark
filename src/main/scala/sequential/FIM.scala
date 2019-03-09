package sequential

import sequential.Apriori.Itemset

trait FIM {

  def findFrequentItemsets(transactions: List[Itemset], minSupport: Int): List[Itemset]

  def execute(transactions: List[Itemset], minSupport: Int): List[Itemset] = {
    val t0 = System.currentTimeMillis()
    val itemsets = findFrequentItemsets(transactions, minSupport)
    val elapsed = (System.currentTimeMillis() - t0) / 1000
    println("Elapsed time: " + elapsed + " seconds")
    itemsets
  }

  def execute(transactions: String, minSupport: Int): List[Itemset] = {
    execute(Util.parseTransactionsByText(transactions), minSupport)
  }

}
