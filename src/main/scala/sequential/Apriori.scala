package sequential

import sequential.Apriori.Itemset
import sequential.Util.printItemsets

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 1. Find all frequent 1-itemsets
  * 2. Find all frequent 2-itemsets, having the last iteration as the input
  * 3. ...
  * 4. Loop until no more frequent itemsets are found
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

class Apriori extends FIM with Serializable {

  var singletons = List[String]()

  def findFrequentItemsets(transactions: List[Itemset], minSupport: Double): List[Itemset] = {
    val support = Util.absoluteSupport(minSupport, transactions.size)
    singletons = findSingletons(transactions, support)
    val frequentItemsets = mutable.Map(1 -> singletons.map(i => List(i)))

    var k = 1
    while (frequentItemsets.get(k).nonEmpty) {
      k += 1
      val candidateKItemsets = findKItemsets(frequentItemsets(k - 1))
      val frequents = filterFrequentItemsets(candidateKItemsets, transactions, support)
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
        if (candidateExistsInTransaction(itemset, t)) {
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
  def findKItemsets(items: List[Itemset]) = {
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
  def isItemsetValid(itemset: List[String], previousItemsets: List[Itemset]): Boolean = {
    for (i <- itemset.indices) {
      val subset = itemset.diff(List(itemset(i)))
      val found = previousItemsets.contains(subset)
      if (!found) {
        return false
      }
    }
    true
  }

  def allElementsEqualButLast(a: List[String], b: List[String]): Boolean = {
    for (i <- 0 until a.size - 1) {
      if (a(i) != b(i))
        return false
    }
    if (a.last == b.last) {
      return false
    }
    true
  }

  def candidateExistsInTransaction(candidate: Itemset, transaction: Itemset): Boolean = {
    // all elements in candidate exist in transaction
    var result = true
    for (elem <- candidate) {
      if (!transaction.contains(elem))
        result = false
    }
    result
  }

  override def findFrequentItemsets(fileName: String, separator: String, transactions: List[Itemset], minSupport: Double): List[Itemset] = {
    if (fileName.isEmpty) {
      findFrequentItemsets(transactions, minSupport)
    }
    else {
      findFrequentItemsets(Util.parseTransactions(fileName, separator), minSupport)
    }
  }

}
