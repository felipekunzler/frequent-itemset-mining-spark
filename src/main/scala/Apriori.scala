import Apriori.Itemset
import Util.printItemsets

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
    val apriori = new Apriori()
    val transactions: List[Itemset] = Util.parseTransactions("/transactions.txt")
    val frequentItemsets = apriori.findFrequentItemsets(transactions, 3)
    printItemsets(frequentItemsets)
  }

}

class Apriori {

  def findFrequentItemsets(transactions: List[Itemset], minSupport: Int): List[Itemset] = {
    val items = findSingletons(transactions, minSupport)
    val frequentItemsets = mutable.Map(1 -> items.map(i => List(i)))

    var k = 1
    while (frequentItemsets.get(k).nonEmpty) {
      val candidateKItemsets = findKItemsets(frequentItemsets(k), k + 1)
      val frequents = filterFrequentItemsets(candidateKItemsets, transactions, minSupport)
      k = k + 1
      if (frequents.nonEmpty) {
        frequentItemsets.update(k, frequents)
      }
    }
    frequentItemsets.values.flatten.toList
  }

  def findFrequentItemsets(transactions: String, minSupport: Int): List[Itemset] = {
    findFrequentItemsets(Util.parseTransactionsByText(transactions), minSupport)
  }

  def filterFrequentItemsets(possibleItemsets: List[Itemset], transactions: List[Itemset], minSupport: Int) = {
    val map = mutable.Map() ++ possibleItemsets.map((_, 0)).toMap
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

  /**
    * A,B
    * B,C
    * B,D
    * =>
    * ABC
    * ABD -> não são frequentes pq AD não foi frequente ali em cima.
    * ACD ->
    * BCD
    */
  private def findKItemsets(items: List[Itemset], n: Int) = {
    // TODO: optimize generation
    val flatItems = items.flatten.distinct
    (new Association().subsets(flatItems) :+ flatItems.sorted)
      .filter(_.size == n)
  }

}
