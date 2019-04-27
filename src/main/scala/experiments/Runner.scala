package experiments

import java.util.{Properties, ResourceBundle}

import sequential.Util.getClass
import sequential._
import sequential.fpgrowth.FPGrowth
import sequential.hashtree.AprioriHashTree
import spark.{DFPS, RApriori, YAFIM, YAFIMHashTree}

import scala.collection.mutable
import scala.io.Source


object Runner {
  def main(args: Array[String]): Unit = {
    new Runner().run()
  }
}

class Runner {

  private val fimInstances = List(
    (0, () => new Apriori),
    (0, () => new AprioriHashTree),
    (0, () => new FPGrowth),
    (0, () => new YAFIM),
    (0, () => new YAFIMHashTree),
    (1, () => new RApriori),
    (1, () => new DFPS))

  private val datasets = List(
    (0, "mushroom.txt", 0.35),
    (1, "pumsb_star.txt", 0.65),
    (1, "chess.txt", 0.85),
    (0, "T10I4D100K.txt", 0.003))

  private val runNTimes = 3
  private val replicateFromTo = 1 to 5

  private val executionTimes: mutable.ListBuffer[mutable.Map[(String, String), List[Long]]] = mutable.ListBuffer()

  def loadConfig(): Unit = {
    val default = getClass.getResourceAsStream("/defaultfim.properties")
    val properties = new Properties()
    properties.load(default)

    println(properties.get("fim.runNTimes"))
  }

  def run(): Unit = {
    loadConfig()
    Util.minPartitions = 8
    val totalRuns = replicateFromTo.size * datasets.count(_._1 == 1) * runNTimes * fimInstances.count(_._1 == 1)
    var currentRun = 1

    replicateFromTo.map(_ - 1).foreach(replicating => {
      executionTimes.append(mutable.LinkedHashMap())
      Util.replicateNTimes = replicating + 1

      datasets.filter(_._1 == 1).foreach(t => {
        for (run <- 1 to runNTimes) {
          fimInstances.filter(_._1 == 1).map(_._2.apply()).foreach(fim => {

            val className = fim.getClass.getSimpleName
            println(s"\nRunning: $className - ${t._2} - $run - x${replicating + 1}")
            println(s"($currentRun / $totalRuns)")
            val frequentSets = fim.execute("/datasets/" + t._2, " ", t._3)
            Util.printItemsets(frequentSets)

            val key = (className, s"${t._2}")
            val executions = executionTimes(replicating).getOrElse(key, List.empty[Long])
            executionTimes(replicating).update(key, executions :+ fim.executionTime)
            currentRun += 1
          })
        }
      })
      printExecutionsForReplication(replicating)
    })

    if (replicateFromTo.size > 1) {
      println("\n==== Final execution times ====\n")
      replicateFromTo.map(_ - 1).foreach(printExecutionsForReplication)
    }
  }

  def printExecutionsForReplication(replication: Int): Unit = {
    val header = Seq("Class ", "Dataset ") ++ (1 to runNTimes).map(i => s" Run $i ") :+ "Mean "
    var prevDataset = ""
    val rows = executionTimes(replication).flatMap(t => {
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
    println(s"\nExecution times replicating ${replication + 1} time(s)\n" + Util.Tabulator.format(header +: rows))
  }

  def formatExecution(value: Double): String = {
    f" ${value / 1000d}%1.2f "
  }

}
