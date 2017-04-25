package org.apache.spark.ml.feature

import scala.math.Ordered

// Represents an object capable of searching starting from an initial state
abstract class Optimizer[T] extends Serializable  {
  def search: EvaluatedState[T]
}

// Represents an object capable of evaluating a given state
abstract class StateEvaluator[T] extends Serializable {
  def evaluate(e: EvaluableState[T]): Double
}

// Represents a state of an optimization
abstract class EvaluableState[T] extends Serializable {
  def data: T
  // def data_= (d: T)
  def size: Int
  def expand: Seq[EvaluableState[T]]

  override def toString(): String = data.toString
}

// Represents a state and its merit
class EvaluatedState[T](val state: EvaluableState[T], val merit: Double)
  extends Ordered[EvaluatedState[T]] with Serializable {

  def compare(that: EvaluatedState[T]) = {
    val compareMerits = this.merit.compare(that.merit)
    if(compareMerits == 0)
      this.state.size.compare(that.state.size) 
    else
      compareMerits
  }

  override def toString(): String = 
    state.toString + " Merit: %.10f".format(merit)
}