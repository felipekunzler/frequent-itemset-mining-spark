import org.scalatest.FunSuite
import org.scalatest.exceptions.TestFailedException
import sequential.Apriori.Itemset
import sequential._
import sequential.fpgrowth.FPGrowth
import spark.YAFIM

class SparkFIMTest extends FunSuite {

  private val fimInstances: Set[FIM] = Set(new FPGrowth(), new YAFIM())
  private val sourceOfTruth = new FPGrowth()

  fimInstances.foreach(fim => {
    val className = fim.getClass.getSimpleName

    test(s"$className - Ensure grocery store") {
      val dataset = "/GroceryStoreDataSet.csv"
      val minSupport = 1
      val itemsets = Util.parseTransactions(dataset).take(1000)

      val frequentSets = fim.execute(itemsets, minSupport)
      val expectedItemsets = sourceOfTruth.execute(itemsets, minSupport)
      assertItemsetsMatch(expectedItemsets, frequentSets, className)
    }
  })

  private def assertItemsetsMatch(expected: String, result: List[Itemset], className: String): Unit = {
    val expectedSets = Util.parseTransactionsByText(expected)
    assertItemsetsMatch(expectedSets, result, className)
  }

  private def assertItemsetsMatch(expectedSets: List[Itemset], result: List[Itemset], className: String): Unit = {
    try {
      assert(result.size === expectedSets.size)
      assert(expectedSets.intersect(result).size === expectedSets.size)
    }
    catch {
      case e: TestFailedException => {
        println(s"Expected for $className:")
        Util.printItemsets(expectedSets)
        println("\nResult:")
        Util.printItemsets(result)
        throw e
      }
    }
  }

}
