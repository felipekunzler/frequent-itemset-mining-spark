import FIMTest.assertItemsetsMatch
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import sequential.Apriori.Itemset
import sequential._
import sequential.fpgrowth.FPGrowth
import sequential.hashtree.AprioriHashTree
import spark.{DFPS, RApriori, YAFIM, YAFIMHashTree}

import scala.collection.mutable

class DatasetsFIMTest extends FunSuite with BeforeAndAfterAll {

  private val fimInstances = List(
    (0, () => new Apriori),
    (1, () => new AprioriHashTree),
    (1, () => new FPGrowth),
    (1, () => new YAFIM),
    (1, () => new YAFIMHashTree),
    (1, () => new RApriori),
    (1, () => new DFPS))

  private val datasets = List(
    (1, "mushroom.txt", 0.35),
    (1, "pumsb_star.txt", 0.65),
    (1, "chess.txt", 0.85),
    (1, "T10I4D100K.txt", 0.03))

  private val runNTimes = 3

  private val executionTimes: mutable.Map[(String, String), List[Long]] = mutable.LinkedHashMap()
  private val resultsCache: mutable.Map[String, List[Itemset]] = mutable.Map()

  datasets.filter(_._1 == 1).foreach(t => {
    for (run <- 1 to runNTimes) {
      fimInstances.filter(_._1 == 1).map(_._2.apply()).foreach(fim => {

        val className = fim.getClass.getSimpleName
        test(s"$className - ${t._2} - $run") {
          val frequentSets = fim.execute("/datasets/" + t._2, " ", t._3)
          Util.printItemsets(frequentSets)

          if (!resultsCache.contains(t._2 + t._3))
            resultsCache.update(t._2 + t._3, frequentSets)
          val expectedItemsets = resultsCache(t._2 + t._3)

          val key = (className, s"${t._2}")
          val executions = executionTimes.getOrElse(key, List.empty[Long])
          executionTimes.update(key, executions :+ fim.executionTime)

          assertItemsetsMatch(expectedItemsets, frequentSets, className)
        }
      })
    }
  })

  override def afterAll() {
    val header = Seq("Class ", "Dataset ") ++ (1 to runNTimes).map(i => s" Run $i ") :+ "Mean "
    var prevDataset = ""
    val rows = executionTimes.flatMap(t => {
      val mean = t._2.sum / runNTimes
      val r = List(Seq(s" ${t._1._1} ", s" ${t._1._2} ") ++ t._2.map(formatExecution(_)) :+ formatExecution(mean))
      if (prevDataset != t._1._2 && !prevDataset.isEmpty) {
        prevDataset = t._1._2
        1.to(runNTimes + 3, 1).map(_ => "") +: r
      }
      else {
        prevDataset = t._1._2
        r
      }
    }).toSeq
    println("\nExecution times:\n" + Util.Tabulator.format(header +: rows))
  }

  def formatExecution(value: Double): String = {
    f" ${value / 1000d}%1.2f "
  }

}
