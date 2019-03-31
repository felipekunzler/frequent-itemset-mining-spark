package sequential

import sequential.Apriori.Itemset
import sequential.Util.printItemsets

import scala.collection.mutable

object NaiveApriori {

  type Itemset = List[String]

  def main(args: Array[String]): Unit = {
    val transactions: List[Itemset] = Util.parseTransactions("/GroceryStoreDataSet.csv")
    val frequentItemsets = new NaiveApriori().execute(transactions, 3)
    printItemsets(frequentItemsets)
  }

}

class NaiveApriori extends FIM {

  override def findFrequentItemsets(fileName: String, separator: String, transactions: List[Itemset], minSupport: Double): List[Itemset] = {
    val support = Util.absoluteSupport(minSupport, transactions.size)
    val items = findSingletons(transactions, support)
    val frequentItemsets = mutable.Map(1 -> items.map(i => List(i)))

    var k = 1
    while (frequentItemsets.get(k).nonEmpty) {
      k = k + 1
      val candidateKItemsets = findKItemsetsNaive(frequentItemsets(k - 1), k)
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

  private def findKItemsetsNaive(items: List[Itemset], n: Int) = {
    val flatItems = items.flatten.distinct
    (new NaiveFIM().subsets(flatItems) :+ flatItems.sorted)
      .filter(_.size == n)
  }

}
