import FIMTest.assertItemsetsMatch
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import sequential.Apriori.Itemset
import sequential._
import sequential.fpgrowth.FPGrowth
import sequential.hashtree.AprioriHashTree
import spark.{DFPS, YAFIM, YAFIMHashTree}

import scala.collection.mutable

class DatasetsFIMTest extends FunSuite with BeforeAndAfterAll {

  private val fimInstances: List[FIM] = List(new YAFIMHashTree, new DFPS)

  private val executionTimes: mutable.ListBuffer[(String, String, Long)] = mutable.ListBuffer()
  private val resultsCache: mutable.Map[String, List[Itemset]] = mutable.Map()

  List(("mushroom.txt", 0.35), ("pumsb_star.txt", 0.65), ("chess.txt", 0.85), ("T10I4D100K.txt", 0.03)).foreach(t => {
    fimInstances.foreach(fim => {
      val className = fim.getClass.getSimpleName
      test(s"$className - ${t._1}") {
        val frequentSets = fim.execute("/datasets/" + t._1, " ", t._2)
        Util.printItemsets(frequentSets)

        if (!resultsCache.contains(t._1 + t._2))
          resultsCache.update(t._1 + t._2, frequentSets)

        val expectedItemsets = resultsCache(t._1 + t._2)
        executionTimes.append((className, s"${t._1}", fim.executionTime))
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
