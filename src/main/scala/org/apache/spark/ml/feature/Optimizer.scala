package org.apache.spark.ml.feature

import scala.math.Ordered

// Represents an object capable of searching starting from an initial state
abstract class Optimizer extends Serializable  {
  def search: EvaluatedState
}

// Represents an object capable of evaluating a given collection of states
abstract class StateEvaluator extends Serializable {
  def preEvaluate(states: Seq[EvaluableState]): Unit
  def evaluate(state: EvaluableState): EvaluatedState
}

// Represents a state of an optimization
abstract class EvaluableState extends Serializable {
  // def data: T
  // def data_= (d: T)
  def size: Int
  def expand: Seq[EvaluableState]
  // override def toString(): String = data.toString
}

// Represents a state and its merit
class EvaluatedState(val state: EvaluableState, val merit: Double)
  extends Ordered[EvaluatedState] with Serializable {

  def compare(that: EvaluatedState) = {
    val compareMerits = this.merit.compare(that.merit)
    if(compareMerits == 0)
      this.state.size.compare(that.state.size) 
    else
      compareMerits
  }

  override def toString(): String = 
    state.toString + " Merit: %.10f".format(merit)
}