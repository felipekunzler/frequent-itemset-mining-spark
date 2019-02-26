package sequential

import sequential.Apriori.Itemset
import sequential.NaiveFIM.Rule
import sequential.Util.printItemsets

import scala.collection.mutable

/**
  * 1. Gerar combinações de itemsets candidatos
  * 2. Filtrar de acordo com suporte minimo
  */
object NaiveFIM {

  type Rule = (List[String], List[String])

  def main(args: Array[String]): Unit = {
    val transactions: List[List[String]] = Util.parseTransactions("/transactions.txt")
    printItemsets(new NaiveFIM().findFrequentItemsets(transactions, 3))
  }

}

class NaiveFIM extends FIM {

  def findFrequentItemsets(transactions: List[Itemset], minSupport: Int): List[Itemset] = {
    val items = transactions.flatten.distinct
    val candidateItemsets = subsets(items) :+ items.sorted
    new Apriori().filterFrequentItemsets(candidateItemsets, transactions, minSupport)
  }

  /**
    * A, B, C
    *
    * A, B
    * A, C
    * B, C
    * A
    * B
    * C
    */
  def subsets(set: List[String]): List[List[String]] = {
    var sets = List[List[String]]()
    val n = set.length
    // 2^n - 1
    for (i <- 1 until (1 << n) - 1) {
      var subset = List[String]()
      var m = 1; // m is used to check set bit in binary representation.
      for (j <- 0 until n) {
        if ((i & m) > 0) {
          subset = subset :+ set(j)
        }
        m = m << 1
      }
      sets = sets :+ subset.sorted
    }
    sets
  }

  def findFrequentRules(transactions: List[List[String]], minSupport: Int) = {
    val rules = mutable.Map[Rule, Int]()
    for (rule <- combinations(transactions)) {
      val count = rules.getOrElse(rule, 0)
      rules.update(rule, count + 1)
    }

    rules.filter(t => t._2 >= minSupport)
      .foreach(t => println(Util.formatRule(t._1) + f" [${t._2}]"))
  }

  /**
    * Given A, B, C
    *
    * A -> B, C
    * A -> B
    * A -> C
    *
    * B -> A, C
    * B -> A
    * B -> C
    *
    * C -> A, B
    * C -> A
    * C -> B
    *
    * A, B -> C
    * A, C -> B
    * B, C -> A
    *
    * 1. Gera todos os subsets
    * 2. Para cada set, usa como left hand, e gera todos substes sem o que está na esquerda
    */
  def combinations(transactions: List[List[String]]): List[Rule] = {
    var combinations = List[Rule]()
    for (t <- transactions) {
      val ss = subsets(t)
      for (leftHand <- ss) {
        for (rightHand <- ss) {
          if (rightHand.intersect(leftHand).isEmpty) {
            combinations = combinations :+ (leftHand, rightHand)
          }
        }
      }
    }
    combinations
  }

}
