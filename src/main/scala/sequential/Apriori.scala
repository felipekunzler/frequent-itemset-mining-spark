package sequential

import sequential.Apriori.Itemset
import sequential.Util.printItemsets

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 1. Encontrar 1-itemsets frequentes
  * 2. Encontrar 2-itemsets frequentes, tendo a iteração anterior como input
  * 3. ...
  * 4. Até não ter mais itemsets frequentes
  */
object Apriori {

  type Itemset = List[String]

  def main(args: Array[String]): Unit = {
    var transactions: List[Itemset] = Util.parseTransactions("/GroceryStoreDataSet.csv")
    transactions = Util.parseTransactionsByText(
      """
        |1,3
        |1,2,3
        |1
        |1,3,4
        |3
        |1,2
        |1,2,3,4
      """.stripMargin)
    val frequentItemsets = new Apriori().execute(transactions, 3)
    printItemsets(frequentItemsets)
  }

}

class Apriori extends FIM {

  override def findFrequentItemsets(transactions: List[Itemset], minSupport: Int): List[Itemset] = {
    val items = findSingletons(transactions, minSupport)
    val frequentItemsets = mutable.Map(1 -> items.map(i => List(i)))

    var k = 1
    while (frequentItemsets.get(k).nonEmpty) {
      k = k + 1
      val candidateKItemsets = findKItemsets(frequentItemsets(k - 1))
      val frequents = filterFrequentItemsets(candidateKItemsets, transactions, minSupport)
      if (frequents.nonEmpty) {
        frequentItemsets.update(k, frequents)
      }
    }
    frequentItemsets.values.flatten.toList
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
      .keySet.toList.sorted
  }

  /**
    * Given that all itemsets are sorted, merge two itemsets if all items are the same but the last.
    * Example:
    * in: {AB}, {AC}, {BC}, {BD}
    * generates: {ABC}, {BCD}
    * after pruning: {ABC}
    */
  private def findKItemsets(items: List[Itemset]) = {
    val nextKItemsets = ArrayBuffer[Itemset]()
    for (i <- items.indices) {
      for (j <- i + 1 until items.size) {
        if (items(i).size == 1 || allElementsEqualButLast(items(i), items(j))) {
          val newItemset = (items(i) :+ items(j).last).sorted
          // Prune new itemsets that are not frequent by checking all k-1 itemsets
          if (isItemsetValid(newItemset, items))
            nextKItemsets += newItemset
        }
      }
    }
    nextKItemsets.toList
  }

  /**
    * Performs pruning by checking if all subsets of the new itemset exist within
    * the k-1 itemsets.
    * Do all subsets need to be checked or only those containing n-1 and n-2?
    */
  private def isItemsetValid(itemset: List[String], previousItemsets: List[Itemset]): Boolean = {
    for (i <- itemset.indices) {
      val subset = itemset.diff(List(itemset(i)))
      val found = previousItemsets.contains(subset)
      if (!found) {
        return false
      }
    }
    true
  }

  private def allElementsEqualButLast(a: List[String], b: List[String]): Boolean = {
    for (i <- 0 until a.size - 1) {
      if (a(i) != b(i))
        return false
    }
    if (a.last == b.last) {
      return false
    }
    true
  }

}
