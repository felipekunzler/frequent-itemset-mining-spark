import Apriori.Itemset
import Association.Rule

import scala.collection.immutable.SortedMap
import scala.io.Source

object Util {

  def parseTransactions(lines: List[String]): List[Itemset] = {
    lines.filter(l => !l.startsWith("#"))
      .filter(!_.trim.isEmpty)
      .map(l => l.split(","))
      .map(l => l.map(item => item.trim).toList)
  }

  def parseTransactions(fileName: String): List[Itemset] = {
    parseTransactions(
      Source.fromInputStream(getClass.getResourceAsStream(fileName)).getLines.toList
    )
  }

  def parseTransactionsByText(text: String): List[Itemset] = {
    parseTransactions(text.split("\n").toList)
  }

  def formatRule(rule: Rule): String = {
    "{" + rule._1.mkString(", ") + "} -> {" + rule._2.mkString(", ") + "}"
  }

  def printItemsets(itemsets: List[Itemset]) = {
    SortedMap(itemsets.groupBy(itemset => itemset.size).toSeq:_*)
      .mapValues(i => i.map(set => s"{${set.mkString(", ")}}").mkString(", "))
      .foreach(t => println(s"[${t._1}] ${t._2}"))
  }

}
