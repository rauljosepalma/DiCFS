package org.apache.spark.ml.feature

import scala.collection.mutable.ListBuffer

// Implements an object capable of doing a greedy best first search
class BestFirstSearcher(
  initialState: EvaluableState,
  evaluator: StateEvaluator,
  maxFails: Int) extends Optimizer {

  def search: EvaluatedState = {
    val evaluatedInitState = evaluator.evaluate(initialState)

    doSearch(
      new BestFirstSearchList(maxFails) += evaluatedInitState, 
      evaluatedInitState, 0)
  }

  // The received queue must always have at least one element
  private def doSearch(
    priorityList: BestFirstSearchList,
    bestState: EvaluatedState,
    nFails: Int): EvaluatedState = {

    // Remove head (best priority) and expand it
    val head = priorityList.dequeue
    
    // DEBUG
    // println(s"BEST STATE = {${bestState.state.toString}}")
    // println(s"BEST MERIT = ${bestState.merit}")
    // println(s"HEAD       = {${head.state.toString}}")

    // A collection of evaluated search states
    val newStates: Seq[EvaluableState] = head.state.expand

    // The hard-work!    
    evaluator.preEvaluate(newStates)

    val newEvaluatedStates: Seq[EvaluatedState] = 
      newStates.map(evaluator.evaluate(_))
      
    // Interestlingly enough, the search on WEKA accepts repeated elements on
    // the list, so this behavior is copied here.
    priorityList ++= newEvaluatedStates

    if (priorityList.isEmpty) {
      // DEBUG
      println("EMPTY LIST!")

      bestState
    } else {
      val bestNewState = priorityList.head
      if (bestNewState.merit - bestState.merit > 0.00001) {
        doSearch(priorityList, bestNewState, 0)
      } else if (nFails < this.maxFails - 1) {
        
        // DEBUG
        println("FAIL++: " + (nFails + 1).toString)

        doSearch(priorityList, bestState, nFails + 1)
      } else {
        bestState
      }
    }
  }
}

class BestFirstSearchList(capacity: Int) {

  require(capacity > 0, "SearchList capacity must be greater than 0")

  private val data = ListBuffer.empty[EvaluatedState]

  def +=(ne: EvaluatedState): BestFirstSearchList = {
    
    // If the list is full
    if (data.size == capacity) {
      // Ignore elements with lower merit than the lowest when list its full
      if (data.last.merit >= ne.merit) {
        // println("SKIPPING ELEMENT!!" + ne.toString)
        return this
      }
    
      // Add the element preserving the order
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

    // DEBUG
    // println(this.toString)

    return this
  }

  def ++=(nes: Seq[EvaluatedState]): Unit = {
    nes.foreach{ ne => this += ne }
  }

  def dequeue: EvaluatedState = {
    val head = data.head
    data.trimStart(1)
    head
  }

  def head: EvaluatedState = data.head

  def isEmpty: Boolean = data.isEmpty

  override def toString(): String =  {
    data.mkString("{", ", ", "}")
  } 

}