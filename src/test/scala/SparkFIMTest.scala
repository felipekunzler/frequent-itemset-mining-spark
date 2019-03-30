import java.util.UUID

import org.scalatest.exceptions.TestFailedException
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import sequential.Apriori.Itemset
import sequential._
import sequential.fpgrowth.FPGrowth
import spark.YAFIM

import scala.collection.mutable

class SparkFIMTest extends FunSuite with BeforeAndAfterAll {

  private val fimInstances: Set[FIM] = Set(new FPGrowth(), new Apriori(), new Apriori(), new YAFIM(), new YAFIM())
  private val sourceOfTruth = new FPGrowth()
  private val executionTimes: mutable.ListBuffer[(String, String, Long)] = mutable.ListBuffer()

  fimInstances.foreach(fim => {
    val className = fim.getClass.getSimpleName

    test(s"$className - Ensure grocery store - ${UUID.randomUUID().toString}") {
      val dataset = "/GroceryStoreDataSet.csv"
      val minSupport = 1
      val itemsets = Util.parseTransactions(dataset)

      val frequentSets = fim.execute(itemsets, minSupport)
      val expectedItemsets = sourceOfTruth.execute(itemsets, minSupport)
      assertItemsetsMatch(expectedItemsets, frequentSets, className)

      executionTimes.append((className, s"$dataset (${itemsets.size})", fim.executionTime))
    }
  })

  override def afterAll() {
    val table = Seq("Class", "Dataset", "Execution") +: executionTimes.map(t => {
      Seq(s" ${t._1} ", s" ${t._2} ", f" ${t._3 / 1000d}%1.2f ")
    })
    println("\nExecution times:\n" + Util.Tabulator.format(table))
  }

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
