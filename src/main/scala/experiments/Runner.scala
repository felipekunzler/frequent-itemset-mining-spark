package experiments

import java.util.Properties

import sequential._
import sequential.fpgrowth.FPGrowth
import sequential.hashtree.AprioriHashTree
import spark.{DFPS, RApriori, YAFIM, YAFIMHashTree}

import scala.collection.mutable


object Runner {
  def main(args: Array[String]): Unit = {
    new Runner().run()
  }

  var clusterMode: Boolean = false

}

/**
 * Helper class to run the experiments. 
 */
class Runner {

  val fimProperties = getClass.getResourceAsStream("/fim.properties")
  val defaultProperties = getClass.getResourceAsStream("/defaultfim.properties")

  val properties = new Properties()
  properties.load(if (fimProperties != null) fimProperties else defaultProperties)

  private val runNTimes = getInt("fim.runNTimes")
  private val replicatingDataset: Array[Int] = properties.getProperty("fim.datasetReplicating").split(",")
    .map(_.trim)
    .map(_.toInt)

  private val executionTimes: mutable.Map[Int, mutable.Map[(String, String), List[Long]]] = mutable.Map()

  Runner.clusterMode = properties.getProperty("fim.clusterMode", "false").toBoolean

  def run(): Unit = {
    Util.minPartitions = getInt("fim.minPartitions")
    Util.props = properties

    // Spark warm-up run
    val path = properties.getProperty("fim.dataset.mushroom.path")
    new spark.YAFIM().execute(path, " ", .99)

    val totalRuns = replicatingDataset.length * datasets.size * runNTimes * fimInstances.count(_._1 == 1)
    var currentRun = 1

    replicatingDataset.foreach(replicating => {
      executionTimes.put(replicating, mutable.LinkedHashMap())
      Util.replicateNTimes = replicating

      datasets.foreach(t => {
        for (run <- 1 to runNTimes) {
          fimInstances.filter(_._1 == 1).map(_._2.apply()).foreach(fim => {

            val className = fim.getClass.getSimpleName
            Util.appName = s"$className - ${t._1} - x$replicating - $run"
            println(s"\nRunning: ${Util.appName}")
            println(s"($currentRun / $totalRuns)")
            fim.execute(t._2, " ", t._3)
            //Util.printItemsets(frequentSets)

            val key = (className, s"${t._1}")
            val executions = executionTimes(replicating).getOrElse(key, List.empty[Long])
            executionTimes(replicating).update(key, executions :+ fim.executionTime)
            currentRun += 1
          })
        }
      })
      printExecutionsForReplication(replicating)
    })

    if (replicatingDataset.length > 1) {
      println("\n==== Final execution times ====\n")
      replicatingDataset.foreach(printExecutionsForReplication)
    }
  }

  def printExecutionsForReplication(replication: Int): Unit = {
    val header = Seq("Class ", "Dataset ") ++ (1 to runNTimes).map(i => s" Run $i ") ++ List("Mean ", "SD ")
    var prevDataset = ""
    val rows = executionTimes(replication).flatMap(t => {
      val r = List((Seq(s" ${t._1._1} ", s" ${t._1._2} ") ++ t._2.map(formatExecution(_)) :+ formatExecution(mean(t._2))) :+ formatExecution(stdDev(t._2)))
      if (prevDataset != t._1._2 && !prevDataset.isEmpty) {
        prevDataset = t._1._2
        1.to(runNTimes + 4, 1).map(_ => "") +: r
      }
      else {
        prevDataset = t._1._2
        r
      }
    }).toSeq
    println(s"\nExecution times replicating $replication time(s)\n" + Util.Tabulator.format(header +: rows))
    printSimpleExecutionsForReplication(replication)
  }

  def printSimpleExecutionsForReplication(replication: Int): Unit = {
    val header = "" +: fimInstances.filter(_._1 == 1).map(_._2.apply().getClass.getSimpleName).map(i => s" $i ")
    val sets = datasets.map(_._1)
    val rows = sets.flatMap(set => {
      // assuming that execution times has same order as header
      val row = s" $set " +: executionTimes(replication).filter(t => t._1._2 == set).map(t => {
        val mean = t._2.sum / runNTimes
        formatExecution(mean)
      }).toList
      List(row)
    })

    println(s"\n" + Util.Tabulator.format(header +: rows))
  }

  def reverseSeqs(seqs: Seq[Seq[String]]) = {
    var reverseSeqs: Seq[Seq[String]] = Seq()
    for (i <- seqs.head.indices) {
      var reverseSeq: Seq[String] = Seq()
      for (j <- seqs.indices) {
        reverseSeq = reverseSeq :+ seqs(j)(i)
      }
      reverseSeqs = reverseSeqs :+ reverseSeq
    }
    reverseSeqs
  }

  def formatExecution(value: Double): String = {
    f" ${value / 1000d}%1.2f "
  }

  def getInt(prop: String): Int = Integer.parseInt(properties.getProperty(prop))

  private val classes = List(
    ("Apriori", () => new Apriori),
    ("AprioriHashTree", () => new AprioriHashTree),
    ("FPGrowth", () => new FPGrowth),
    ("YAFIM", () => new YAFIM),
    ("YAFIMHashTree", () => new YAFIMHashTree),
    ("RApriori", () => new RApriori),
    ("DFPS", () => new DFPS))

  private val fimInstances = classes.map(c => {
    (getInt(s"fim.class.${c._1}"), c._2)
  })

  private val datasets = List("mushroom", "pumsb_star", "chess", "T10I4D100K")
    .map(d => (d, "fim.dataset." + d))
    .filter(d => getInt(d._2) > 0)
    .map(d => {
      (d._1, properties.getProperty(d._2 + ".path"), properties.getProperty(d._2 + ".support").toDouble)
    })

  import Numeric.Implicits._

  def mean[T: Numeric](xs: Iterable[T]): Double = xs.sum.toDouble / xs.size

  def variance[T: Numeric](xs: Iterable[T]): Double = {
    val avg = mean(xs)

    xs.map(_.toDouble).map(a => math.pow(a - avg, 2)).sum / xs.size
  }

  def stdDev[T: Numeric](xs: Iterable[T]): Double = math.sqrt(variance(xs))

}
