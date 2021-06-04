package sequential

import sequential.Apriori.Itemset

/**
  * Common interface for all FIM implementations. 
  */
trait FIM {

  def findFrequentItemsets(fileName: String = "", separator: String = "", transactions: List[Itemset], minSupport: Double): List[Itemset]

  def execute(transactions: List[Itemset], minSupport: Double): List[Itemset] = {
    executionTime = 0
    val t0 = System.currentTimeMillis()
    val itemsets = findFrequentItemsets("", "", transactions, minSupport)
    if (executionTime == 0)
      executionTime = System.currentTimeMillis() - t0
    println(f"Elapsed time: ${executionTime / 1000d}%1.2f seconds. Class: ${getClass.getSimpleName}. Items: ${transactions.size}")
    itemsets
  }

  def execute(fileName: String, separator: String, minSupport: Double): List[Itemset] = {
    executionTime = 0
    val t0 = System.currentTimeMillis()
    val itemsets = findFrequentItemsets(fileName, separator, List.empty, minSupport)
    if (executionTime == 0)
      executionTime = System.currentTimeMillis() - t0
    println(f"Elapsed time: ${executionTime / 1000d}%1.2f seconds. Class: ${getClass.getSimpleName}.")
    itemsets
  }

  def executeByText(transactions: String, minSupport: Double): List[Itemset] = {
    execute(Util.parseTransactionsByText(transactions), minSupport)
  }

  var executionTime: Long = 0

}
