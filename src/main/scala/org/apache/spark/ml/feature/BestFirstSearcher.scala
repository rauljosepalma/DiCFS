package org.apache.spark.ml.feature

import scala.collection.mutable.ListBuffer

// Implements an object capable of doing a greedy best first search
class BestFirstSearcher[T](
  initialState: EvaluableState[T],
  evaluator: StateEvaluator[T],
  maxFails: Int) extends Optimizer[T] {

  def search: EvaluatedState[T] = {
    val evaluatedInitState = 
      new EvaluatedState[T](initialState, evaluator.evaluate(initialState))

    doSearch(
      new BestFirstSearchList[T](maxFails) += evaluatedInitState, 
      evaluatedInitState, 0)
  }

  // The received queue must always have at least one element
  private def doSearch(
    priorityList: BestFirstSearchList[T],
    bestState: EvaluatedState[T],
    nFails: Int): EvaluatedState[T] = {

    // DEBUG
    // println("LIST: " + priorityList.toString)
    
    // Remove head (best priority) and expand it
    val head = priorityList.dequeue
    // A collection of evaluated search states
    // DEBUG
    // println("HEAD:" + head.toString)
    val newStates: IndexedSeq[EvaluatedState[T]] = 
      (head
        .state
        .expand
        .map(s => new EvaluatedState[T](s,evaluator.evaluate(s)))
      )
      
    // Interestlingly enough, the search on WEKA accepts repeated elements on
    // the list, so this behavior is copied here.
    priorityList ++= newStates

    if (priorityList.isEmpty) {
      bestState
    } else {
      val bestNewState = priorityList.head
      if (bestNewState.merit - bestState.merit > 0.00001) {
        doSearch(priorityList, bestNewState, 0)
      } else if (nFails < this.maxFails - 1) {
        
        // DEBUG
        // println("FAIL++: " + (nFails + 1).toString)

        doSearch(priorityList, bestState, nFails + 1)
      } else {
        bestState
      }
    }
  }
}

class BestFirstSearchList[T](capacity: Int) {

  require(capacity > 0, "SearchList capacity must be greater than 0")

  private val data = ListBuffer.empty[EvaluatedState[T]]

  def +=(ne: EvaluatedState[T]): BestFirstSearchList[T] = {
    
    // If the list is full
    if (data.size == capacity) {
      // Ignore elements with lower merit than the lowest when list its full
      if (data.last.merit >= ne.merit) {
        // println("SKIPPING ELEMENT!!" + ne.toString)
        return this
      }
    
      // Add the element preserbing the order
      val index = data.lastIndexWhere((s) => s.merit >= ne.merit)
      // If the new element should go last, replace the last
      if (index == data.size - 1) {
        data(data.size - 1) = ne
      // If not the last, add the element and remove last
      } else {
        data.insert(index + 1, ne)
        data.trimEnd(1)
      }

    // If there is still space in the list
    } else {
      val index = data.lastIndexWhere((s) => s.merit >= ne.merit)
      data.insert(index + 1, ne)
    }

    println(this.toString)

    return this
  }

  def ++=(nes: TraversableOnce[EvaluatedState[T]]): Unit = {
    nes.foreach{ ne => this += ne }
  }

  def dequeue: EvaluatedState[T] = {
    val head = data.head
    data.trimStart(1)
    head
  }

  def head: EvaluatedState[T] = data.head

  def isEmpty: Boolean = data.isEmpty

  override def toString(): String =  {
    data.mkString("{", ", ", "}")
  } 

}