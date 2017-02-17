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
  def data_= (d: T)
  def expand: IndexedSeq[EvaluableState[T]]

  override def toString(): String = data.toString
}

// Represents a state and its merit
class EvaluatedState[T](val state: EvaluableState[T], val merit: Double) 
  extends Ordered[EvaluatedState[T]] with Serializable {

  def compare(that: EvaluatedState[T]) = {
    if(this.merit - that.merit > 0.0) 1 
    else if (this.merit == that.merit) 0
    else -1
  }

  override def toString(): String = 
    state.toString + " Merit: %.4f".format(merit)
}