package org.apache.spark.ml.feature

import scala.collection.mutable.ListBuffer

// Implements an object capable of doing a greedy best first search
class BestFirstSearcher (
  initialStates: Seq[EvaluableState],
  evaluator: StateEvaluator,
  maxFails: Int) extends Optimizer{

  def search: Seq[EvaluatedState] = {
    val evaluatedInitStates = evaluator.evaluate(initialStates)
    val priorityLists = 
      evaluatedInitStates.map{ (e: EvaluatedState) =>
        new BestFirstSearchList(maxFails) += e
      }

    doSearch(priorityLists, evaluatedInitStates, 
      Seq.fill(initialStates.size)(0))
  }

  // The received queue must always have at least one element
  private def doSearch(
    priorityLists: Seq[BestFirstSearchList],
    bestStates: Seq[EvaluatedState],
    nsFails: Seq[Int]): Seq[EvaluatedState] = {

    // DEBUG
    // println("LIST: " + priorityList.toString)
    
    // Remove head (best priority)
    // A head can be None if list is empty (dequeOption) or if nFails
    // has reached maximum
    val heads: Seq[Option[EvaluatedState]] = 
      priorityLists.zip(nsFails).map{ case (list, nFails) => 
        if (nFails >= maxFails) None
        else list.dequeueOption
      }

    // Expand new states from heads
    val newEvaluableStates: Seq[Option[Seq[EvaluableState]]] = 
      heads.map(_.map(_.state.expand))
    // The main objective of all this change is to evaluate
    // all the needed states in a single call
    evaluator.preEvaluate(newEvaluableStates.flatten.flatten)
    // evaluated states are only obtained for existent evaluable states 
    val newEvaluatedStates: Seq[Option[Seq[EvaluatedState]]] = 
      newEvaluableStates.map(_.map(evaluator.evaluate(_)))

    // Interestlingly enough, the search on WEKA accepts repeated elements on
    // the list, so this behavior is copied here. 
    priorityLists.zip(newEvaluatedStates)
      .foreach{ 
        case (list, Some(states)) => list ++= states 
        case (_, None) => ; // do nothing
      }
    // End when all lists have empty state or have reached maximum fails
    if(priorityLists.zip(nsFails)
        .forall{ case (list, nFails) => list.isEmpty || nFails >= maxFails }){
      bestStates
    }else{
      // Empty priorityLists will return None and bestState is kept
      val (newBestStates, newNFails): (Seq[EvaluatedState],Seq[Int]) = 
        (priorityLists.map(_.headOption), bestStates, nsFails).zipped.map{ 
          case (Some(bestNewState), bestState: EvaluatedState, nFails: Int) =>
            if(bestNewState.merit - bestState.merit > 0.00001)
              (bestNewState, nFails)
            else
              (bestState, nFails + 1)
          case (None, bestState: EvaluatedState, nFails: Int) =>
            (bestState, nFails)
        }.unzip

      doSearch(priorityLists, newBestStates, newNFails)
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

  def dequeueOption: Option[EvaluatedState] = {
    if (!this.isEmpty) Some(dequeue)
    else None
  }

  def head: EvaluatedState = data.head

  def headOption: Option[EvaluatedState] = data.headOption

  def isEmpty: Boolean = data.isEmpty

  override def toString(): String =  {
    data.mkString("{", ", ", "}")
  } 

}