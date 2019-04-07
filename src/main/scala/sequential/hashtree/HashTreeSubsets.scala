package sequential.hashtree

import sequential.Apriori
import sequential.Apriori.Itemset

import scala.collection.mutable


// TODO: Is only one hash tree shipped to each server?
// TODO: Built tree of size 3 and rows 24 in 0 -> Searched all tree in 85 [Something went wrong here]
// TODO: Check ratio of buckets and hit buckets too

class HashTreeSubsets(val candidates: List[Itemset]) {

  /** How many levels the hash tree has */
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
    else if (nextNode.candidatesBucket.size < size || currentNode.level >= size - 1) {
      // put into the bucket
      nextNode.candidatesBucket.append(new Pair(candidate, 0))
    }
    else {
      // bucket size exceeded, break into children nodes
      for (elem <- nextNode.candidatesBucket) {
        putCandidate(elem.candidate, nextNode)
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

  def findFrequents(minSupport: Int) = {
    val frequents = mutable.ListBuffer[Itemset]()
    frequentCandidates(rootNode, frequents, minSupport)
    frequents
  }

  def frequentCandidates(node: Node, frequents: mutable.ListBuffer[Itemset], minSupport: Int): Unit = {
    if (node.candidatesBucket.nonEmpty) {
      for (pair <- node.candidatesBucket) {
        if (pair.count >= minSupport) {
          frequents.append(pair.candidate)
        }
      }
    } else {
      for (n <- node.children) {
        frequentCandidates(n._2, frequents, minSupport)
      }
    }
  }

  def incrementCandidatesForSubset(subset: Itemset, node: Node = rootNode): Unit = {
    if (node != null) { // node may not exist for a given transaction
      if (node.candidatesBucket.nonEmpty) {
        for (pair <- node.candidatesBucket) {
          if (apriori.candidateExistsInTransaction(pair.candidate, subset))
            pair.count += 1
        }
      } else {
        val level = node.level
        val item = subset(level)
        val nextNode = findNextNode(item, node)
        incrementCandidatesForSubset(subset, nextNode)
      }
    }
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
    val candidatesBucket = mutable.ListBuffer[Pair]()

  }

  class Pair(val candidate: Itemset, var count: Int = 0)

}


