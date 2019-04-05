package sequential

import sequential.Apriori.Itemset

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object HashTree {
  def main(args: Array[String]): Unit = {
    //val c = "1,4,5; 1,2,4; 4,5,7; 1,2,5; 4,5,8; 1,5,9; 1,3,6; 2,3,4; 5,6,7; 3,4,5; 3,5,6; 3,5,7; 6,8,9; 3,6,7; 3,6,8".replaceAll(";", "\n")
    val c = "1,5; 1,3; 1,7".replaceAll(";", "\n")
    val candidates = Util.parseTransactionsByText(c)
    val hashTree = new HashTree(candidates)
    //val subsets = hashTree.findCandidatesForTransaction(List("1", "2", "3", "5", "6", "9"))
    //println("\nFound subsets: " + subsets)
  }
}

// TODO: Is only one hash tree shipped to each server?
class HashTree(val candidates: List[Itemset]) {

  /** How many levels the hash tree has */
  val size = if (candidates.nonEmpty) candidates.head.size else 0
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
    else if (nextNode.candidatesBucket.size < size || currentNode.level >= size - 1) {
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
    //println(s"item: $item; position $position")
    if (currentNode.children.contains(position)) {
      currentNode.children(position)
    }
    else {
      val node = new Node(currentNode.level + 1)
      currentNode.children.update(position, node)
      node
    }
  }

  /**
    * Finds all candidates in the hash tree that are a subset of the given transaction
    */
  def findCandidatesForTransaction(transaction: Itemset): ListBuffer[Itemset] = {
    findCandidatesForTransaction(transaction, 0, rootNode)
      .distinct
      .flatMap(_.candidatesBucket)
      .filter(apriori.candidateExistsInTransaction(_, transaction))
  }

  private def findCandidatesForTransaction(transaction: Itemset, start: Int, currentNode: Node): ListBuffer[Node] = {
    val foundCandidates = new ListBuffer[Node]()

    for (i <- start until Math.min(transaction.size, start + size)) { // Iterate at most <size> times.
      val item = transaction(i)
      val nextNode = findNextNode(item, currentNode)
      if (nextNode != null) { // node may not exist for a given transaction
        if (nextNode.candidatesBucket.nonEmpty) {
          //val matchingCandidates = nextNode.candidatesBucket.filter(c => apriori.candidateExistsInTransaction(c, transaction))
          foundCandidates.append(nextNode)
          //println(s"Found bucket. item $item, ${nextNode.candidatesBucket}")
        }
        else {
          val nextCandidates = findCandidatesForTransaction(transaction, i + 1, nextNode)
          foundCandidates.appendAll(nextCandidates)
        }
      }
    }
    foundCandidates
  }

  val apriori = new Apriori()

  def findNextNode(item: String, currentNode: Node): Node = {
    var position = item.hashCode % size
    if (position == 0) position = size
    if (currentNode.children.contains(position))
      currentNode.children(position)
    else
      null
  }

  class Node(val level: Int) {

    val children = mutable.Map[Int, Node]()
    val candidatesBucket = new ListBuffer[Itemset]()

  }

}


