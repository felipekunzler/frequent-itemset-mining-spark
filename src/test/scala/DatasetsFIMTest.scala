import FIMTest.assertItemsetsMatch
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import sequential.Apriori.Itemset
import sequential._
import sequential.fpgrowth.FPGrowth
import spark.YAFIM

import scala.collection.mutable

class DatasetsFIMTest extends FunSuite with BeforeAndAfterAll {

  private val fimInstances: Set[FIM] = Set(new Apriori(), new YAFIM())

  private val executionTimes: mutable.ListBuffer[(String, String, Long)] = mutable.ListBuffer()
  private val resultsCache: mutable.Map[String, List[Itemset]] = mutable.Map()

  fimInstances.foreach(fim => {
    val className = fim.getClass.getSimpleName

    Set(("mushroom.txt", 0.35), ("chess.txt", 0.85), ("pumsb_star.txt", 0.65), ("T10I4D100K.txt", 0.25)).take(1).foreach(t => {
      test(s"$className - ${t._1}") {
        val itemsets = Util.parseTransactions("/datasets/" + t._1, " ")
        val minSupport = (itemsets.size * t._2 + 0.5).toInt
        val frequentSets = fim.execute(itemsets, minSupport)

        if (!resultsCache.contains(t._1 + t._2))
          resultsCache.update(t._1 + t._2, frequentSets)

        val expectedItemsets = resultsCache(t._1 + t._2)
        executionTimes.append((className, s"${t._1} (${itemsets.size})", fim.executionTime))
        assertItemsetsMatch(expectedItemsets, frequentSets, className)
      }
    })
  })

  override def afterAll() {
    val table = Seq("Class", "Dataset", "Execution") +: executionTimes.map(t => {
      Seq(s" ${t._1} ", s" ${t._2} ", f" ${t._3 / 1000d}%1.2f ")
    })
    println("\nExecution times:\n" + Util.Tabulator.format(table))
  }

}


