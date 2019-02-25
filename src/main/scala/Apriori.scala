import Util.{parseTransactions, printItemsets}

import scala.collection.mutable

/**
  * 1. Encontrar 1-itemsets frequentes
  * 2. Encontrar 2-itemsets frequentes, tendo a iteração anterior como input
  * 3. ...
  * 4. Até não ter mais itemsets frequentes
  */
object Apriori {

  type Itemset = List[String]

  def main(args: Array[String]): Unit = {
    val transactions: List[Itemset] = parseTransactions("/transactions.txt")
    val frequentItemsets = findFrequentItemsets(transactions, 4)
    printItemsets(frequentItemsets)
  }

  def findFrequentItemsets(transactions: List[Itemset], minSupport: Int): List[Itemset] = {
    val items = findSingletons(transactions, minSupport)
    val frequentItemsets = mutable.Map(1 -> items.map(i => List(i)))

    var count = 1
    while (frequentItemsets.get(count).nonEmpty) {
      count = count + 1
      val possibleKItemsets = findKItemsets(frequentItemsets(count - 1), count)
      val frequents = filterFrequentItemsets(possibleKItemsets, transactions, minSupport)
      if (frequents.nonEmpty) {
        frequentItemsets.update(count, frequents)
      }
    }
    frequentItemsets.values.flatten.toList
  }

  def findFrequentItemsets(transactions: String, minSupport: Int): List[Itemset] = {
    findFrequentItemsets(Util.parseTransactionsByText(transactions), minSupport)
  }

  private def filterFrequentItemsets(itemsets: List[Itemset], transactions: List[Itemset], minSupport: Int) = {
    val map = mutable.Map() ++ itemsets.map((_, 0)).toMap
    for (t <- transactions) {
      for ((itemset, count) <- map) {
        // Transactions contains all items from itemset
        if (t.intersect(itemset).size == itemset.size) {
          map.update(itemset, count + 1)
        }
      }
    }
    map.filter(_._2 >= minSupport).keySet.toList
  }

  private def findSingletons(transactions: List[Itemset], minSupport: Int) = {
    transactions.flatten
      .groupBy(identity)
      .map(t => (t._1, t._2.size))
      .filter(_._2 >= minSupport)
      .keySet.toList
  }

  private def findKItemsets(items: List[Itemset], n: Int) = {
    // TODO: optimize generation
    val flatItems = items.flatten.distinct
    (Association.subsets(flatItems) :+ flatItems.sorted)
      .filter(_.size == n)
  }

}
