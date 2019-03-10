package sequential.fpgrowth

import sequential.Apriori.Itemset

import scala.collection.mutable.ArrayBuffer


class FPTree(transactions: List[Itemset], minSupport: Int, singletons: Seq[String]) {

  val rootNode = new FPNode(null, 0, null)

  for (itemset <- transactions) {
    val sortedItemset = itemset.filter(i => singletons.contains(i)).sortWith((a, b) => singletons.indexOf(a) < singletons.indexOf(b))
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
      }
    }
  }

  class FPNode(val item: String, var support: Int, val parent: FPNode) {

    val children: ArrayBuffer[FPNode] = new ArrayBuffer()
    val itemLink: FPNode = null

    def matchingChildren(item: String): Option[FPNode] = {
      children.find(node => node.item == item)
    }

    override def toString = s"FPNode($item, $support)"
  }

}
