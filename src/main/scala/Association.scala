import scala.collection.mutable

/**
  * 1. Gerar combinações de itemsets
  * 2. Filtrar de acordo com suporte minimo
  */
object Association {

  type Rule = (List[String], List[String])

  def main(args: Array[String]): Unit = {
    val transactions: List[List[String]] = Util.parseTransactions("/transactions.txt")
    findFrequentSets(transactions, 4)
  }

  def findFrequentSets(transactions: List[List[String]], minSupport: Int) = {
    val rules = mutable.Map[Rule, Int]()
    for (rule <- combinations(transactions)) {
      val count = rules.getOrElse(rule, 0)
      rules.update(rule, count + 1)
    }

    rules.filter(t => t._2 >= minSupport)
      .foreach(t => println(Util.formatRule(t._1) + f" [${t._2}]"))
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
