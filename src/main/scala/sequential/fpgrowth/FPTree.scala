package sequential.fpgrowth

import sequential.Apriori.Itemset

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


class FPTree(transactions: List[Itemset], minSupport: Int, val singletons: mutable.Map[String, Option[FPNode]]) {

  val rootNode = new FPNode(null, 0, null)

  val t0 = System.currentTimeMillis()

  for (itemset <- transactions) {
    val items = singletons.keys.toList
    val sortedItemset = itemset.filter(i => items.contains(i)).sortWith((a, b) => items.indexOf(a) < items.indexOf(b))
    var currentParentNode = rootNode
    for (item <- sortedItemset) {
      // If path exists, increment, if not, create new node
      val existingNode = currentParentNode.matchingChildren(item)
      if (existingNode.nonEmpty) {
        existingNode.get.support += 1
        currentParentNode = existingNode.get
      }
      else {
        val newNode = new FPNode(item, 1, currentParentNode)
        currentParentNode.children += newNode
        currentParentNode = newNode
        // if not there, put there, otherwise, replace
        if (singletons(item).isEmpty) {
          newNode.lastItemLink = newNode
          singletons.update(item, Option.apply(newNode))
        }
        else {
          val lastItem = singletons(item).get.lastItemLink
          lastItem.itemLink = newNode
          singletons(item).get.lastItemLink = newNode
        }
      }
    }
  }

  println(s"Built fp-tree for ${transactions.size} transactions in ${System.currentTimeMillis() - t0}ms.")

  /*
    * TODO: Currently contains infrequent items (say b:2 for sup 3)
    * TODO: Support prefix with size > 1. Not really needed, only pass the next prefix.
    */
  def conditionalTreeForPrefix(prefix: String, minSupport: Int): FPTree = {
    val conditionalPatternBase = mutable.ListBuffer[Itemset]()

    var bottomNode = singletons(prefix).get
    while (bottomNode != null) {
      var node = bottomNode.parent
      val prefixSupport = bottomNode.support
      val itemset = mutable.ListBuffer[String]()
      while (node != null) {
        itemset.insert(0, node.item)
        node = node.parent
      }
      // TODO: Maybe a hash map with cp and count to prevent duplicates?
      // TODO: Use min support to prune non frequent items somehow, either here or while building the FPTree
      for (_ <- 0 until prefixSupport) {
        conditionalPatternBase.append(itemset.toList)
      }
      bottomNode = bottomNode.itemLink
    }

    val header = mutable.LinkedHashMap(singletons.keySet.toSeq.map(i => i -> Option.empty[FPNode]): _*)
    new FPTree(conditionalPatternBase.toList, minSupport, header)
  }

  def isPrefixFrequent(prefix: String, minSupport: Int): Boolean = {
    var support = 0
    var node = singletons(prefix).orNull
    while (node != null) {
      support += node.support
      node = node.itemLink
    }
    support >= minSupport
  }

}

class FPNode(val item: String, var support: Int, val parent: FPNode) {

  val children: ArrayBuffer[FPNode] = new ArrayBuffer()
  var itemLink: FPNode = _
  var lastItemLink: FPNode = _

  def matchingChildren(item: String): Option[FPNode] = {
    children.find(node => node.item == item)
  }

  override def toString = s"FPNode($item, $support)"
}
