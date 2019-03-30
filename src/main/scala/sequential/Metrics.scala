package sequential

import sequential.Apriori.Itemset
import sequential.fpgrowth.FPGrowth

import scala.collection.mutable.ArrayBuffer

object Metrics {

  def main(args: Array[String]): Unit = {
    val fimInstances = Set(new Apriori(), new FPGrowth())
    fimInstances.foreach(fim => {
      var transactions = new ArrayBuffer[Itemset]()
      for (i <- 0 to 50) {
        transactions = transactions ++ Util.parseTransactions("/GroceryStoreDataSet.csv")
      }
      fim.execute(transactions.toList, 1)
    })

  }

}
