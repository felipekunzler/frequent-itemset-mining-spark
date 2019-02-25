import org.scalatest.FunSuite

class AprioriTest extends FunSuite {

  test("Apriori should return proper frequent itemsets") {
    val itemsets =
      """|a,b,c
         |a,b,c
         |a,b,c
      """.stripMargin
    val frequentSets = Apriori.findFrequentItemsets(itemsets, 3)
    assert(frequentSets.size === 7)
    assert(frequentSets(6) === List("a", "b", "c"))
  }

  test("Apriori Sample") {
    val itemsets =
      """|1,3
         |1,2,3
         |1
         |1,3,4
         |3
         |1,2
         |1,2,3,4
      """.stripMargin
    val frequentSets = Apriori.findFrequentItemsets(itemsets, 3) // sup = (itemsets.size() * 0.4 + 0.5).toInt
    Util.printItemsets(frequentSets)
    val expectedSets = List(
      List("1"), List("2"), List("3"),
      List("1", "3"),
      List("1", "2")
    )
    assert(frequentSets.size === expectedSets.size)
    assert(expectedSets.intersect(frequentSets).size === expectedSets.size)
  }

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
