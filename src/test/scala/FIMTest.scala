import FIMTest.assertItemsetsMatch
import org.scalatest.FunSuite
import org.scalatest.exceptions.TestFailedException
import sequential.Apriori.Itemset
import sequential._
import sequential.fpgrowth.FPGrowth
import spark.YAFIM

class FIMTest extends FunSuite {

  private val fimInstances: Set[FIM] = Set(new NaiveFIM(), new NaiveApriori(), new Apriori(), new FPGrowth(), new YAFIM())
  private val sourceOfTruth = new FPGrowth()

  fimInstances.foreach(fim => {
    val className = fim.getClass.getSimpleName

    test(s"$className - Should return proper frequent itemsets") {
      val itemsets =
        """|a,b,c
           |a,b,c
           |a,b,c
        """.stripMargin

      val expectedItemsets =
        """|a,b,c
           |a,b
           |b,c
           |a,c
           |a
           |b
           |c
        """.stripMargin

      val frequentSets = fim.executeByText(itemsets, 3)
      assertItemsetsMatch(expectedItemsets, frequentSets, className)
    }

    test(s"$className - Many k-itemsets") {
      val itemsets =
        """|1,3
           |1,2,3
           |1
           |1,3,4
           |3
           |1,2
           |1,2,3,4
        """.stripMargin

      val expectedItemsets =
        """
          |1
          |2
          |3
          |1,3
          |1,2
        """.stripMargin

      val frequentSets = fim.executeByText(itemsets, 3) // sup = (itemsets.size() * 0.4 + 0.5).toInt
      assertItemsetsMatch(expectedItemsets, frequentSets, className)
    }

    test(s"$className - Ensure single item") {
      val itemsets =
        """
          |1,2,3
        """.stripMargin

      val minSupport = 1
      val frequentSets = fim.executeByText(itemsets, minSupport)
      val expectedItemsets = sourceOfTruth.executeByText(itemsets, minSupport)
      assertItemsetsMatch(expectedItemsets, frequentSets, className)
    }

    test(s"$className - Ensure grocery store") {
      val itemsets = Util.parseTransactions("/GroceryStoreDataSet.csv").take(1000)
      val minSupport = 1
      val frequentSets = fim.execute(itemsets, minSupport)
      val expectedItemsets = sourceOfTruth.execute(itemsets, minSupport)
      assertItemsetsMatch(expectedItemsets, frequentSets, className)
    }

  })

  test("itemsets comparison") {
    val items1 = List(
      List("1", "3"),
      List("1", "2")
    )
    val items2 = List(
      List(new String("1"), "2"),
      List("1", "3")
    )
    assert(items1.size === items2.size)
    assert(items1.intersect(items2).size === items2.size)
  }

}

object FIMTest {

  def assertItemsetsMatch(expected: String, result: List[Itemset], className: String): Unit = {
    val expectedSets = Util.parseTransactionsByText(expected)
    assertItemsetsMatch(expectedSets, result, className)
  }

  def assertItemsetsMatch(expectedSets: List[Itemset], result: List[Itemset], className: String): Unit = {
    try {
      assert(result.size == expectedSets.size)
      assert(expectedSets.intersect(result).size == expectedSets.size)
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
