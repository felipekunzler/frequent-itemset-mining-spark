package sequential

import sequential.Apriori.Itemset

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object HashTree {
  def main(args: Array[String]): Unit = {
    val c = "1,4,5; 1,2,4; 4,5,7; 1,2,5; 4,5,8; 1,5,9; 1,3,6; 2,3,4; 5,6,7; 3,4,5; 3,5,6; 3,5,7; 6,8,9; 3,6,7; 3,6,8".replaceAll(";", "\n")
    val candidates = Util.parseTransactionsByText(c)
    val hashTree = new HashTree(candidates)
    println(hashTree)
  }
}

class HashTree(val candidates: List[Itemset]) {

  val size = candidates.head.size
  val rootNode = new Node(0)

  for (candidate <- candidates) {
    putCandidate(candidate, rootNode)
  }

  private def putCandidate(candidate: Itemset, currentNode: Node): Unit = {
    val nextNode = getNextNode(candidate, currentNode)
    if (nextNode.children.nonEmpty) {
      // go to the next tree level
      putCandidate(candidate, nextNode)
    }
    else if (nextNode.candidatesBucket.size < size) {
      // put into the bucket
      nextNode.candidatesBucket.append(candidate)
    }
    else {
      // bucket size exceeded, break into children nodes
      for (elem <- nextNode.candidatesBucket) {
        putCandidate(elem, nextNode)
      }
      putCandidate(candidate, nextNode)
      nextNode.candidatesBucket.clear()
    }
  }

  /**
    * Given a candidate and its current node, select which node it belongs to
    * in the next level of the tree. If doesn't already exists, create it.
    */
  private def getNextNode(candidate: Itemset, currentNode: Node): Node = {
    val item = candidate(currentNode.level)
    var position = item.hashCode % size
    if (position == 0) position = size
    if (currentNode.children.contains(position)) {
      currentNode.children(position)
    }
    else {
      val node = new Node(currentNode.level + 1)
      currentNode.children.update(position, node)
      node
    }
  }

  class Node(val level: Int) {

    val children = mutable.Map[Int, Node]()
    val candidatesBucket = ListBuffer[Itemset]()

  }

}


