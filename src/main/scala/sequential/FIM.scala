package sequential

import sequential.Apriori.Itemset

trait FIM {

  def findFrequentItemsets(transactions: List[Itemset], minSupport: Int): List[Itemset]

  def execute(transactions: List[Itemset], minSupport: Int): List[Itemset] = {
    executionTime = 0
    val t0 = System.currentTimeMillis()
    val itemsets = findFrequentItemsets(transactions, minSupport)
    if (executionTime == 0)
      executionTime = System.currentTimeMillis() - t0
    println(f"Elapsed time: ${executionTime/1000d}%1.2f seconds. Class: ${getClass.getSimpleName}. Items: ${transactions.size}")
    itemsets
  }

  def execute(transactions: String, minSupport: Int): List[Itemset] = {
    execute(Util.parseTransactionsByText(transactions), minSupport)
  }

  var executionTime: Long = 0

}
