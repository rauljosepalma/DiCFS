package org.apache.spark.ml.feature

import scala.collection.mutable.PriorityQueue

// Implements an object capable of doing a greedy best first search
class BestFirstSearcher[T](
  initialState: EvaluableState[T],
  evaluator: StateEvaluator[T],
  maxFails: Int) extends Optimizer[T](evaluator) {

  def search: EvaluableState[T] = {
    val evaluatedInitState = 
      new EvaluatedState[T](initialState, Double.NegativeInfinity)

    doSearch(
      PriorityQueue.empty[EvaluatedState[T]] += evaluatedInitState,
      evaluatedInitState, 0)
  }

  // The received queue must always have at least one element
  private def doSearch(
    queue: PriorityQueue[EvaluatedState[T]],
    bestState: EvaluatedState[T],
    nFails: Int): EvaluableState[T] = {

    
    // Remove head and expand it
    val head = queue.dequeue()
    // A collection of evaluated search states
    val newStates: IndexedSeq[EvaluatedState[T]] = 
      head.state.expand.map(s => new EvaluatedState(s, evaluator.evaluate(s)))

    // DEBUG
    println("REMOVING HEAD:" + head.merit + " " + head.toString)
    // println("EXPANDED STATES: ")
    // newStates.foreach(s => println(s.merit + " " + s.toString))
    // println("QUEUE BEFORE NEW STATES: ")
    // println(queue.toString)

    // Add new states to queue (if not already in)
    var addedElements = false
    newStates.foreach{ ns => 
      if(!queue.exists{ 
          (s) => s.state.evaluableData == ns.state.evaluableData }) { 

        queue += ns
        addedElements = true
        }
    }
    // queue ++= newStates
    
    // println("QUEUE AFTER NEW STATES: ")
    // println(queue.toString)
    
    // LAST_LINE checking if this condition is correct!
    if (queue.isEmpty) {
      bestState.state
    } else {
      val bestNewState = queue.head
      if (bestNewState.merit > bestState.merit) {
        
        // DEBUG
        println("NEW BEST STATE: \n" + bestNewState.merit)

        doSearch(queue, bestNewState, 0)
      } else if (nFails < this.maxFails) {
        
        // DEBUG
        println("FAIL++: " + (nFails + 1).toString)

        doSearch(queue, bestState, nFails + 1)
      } else {
        bestState.state
      }
    }
  }
}