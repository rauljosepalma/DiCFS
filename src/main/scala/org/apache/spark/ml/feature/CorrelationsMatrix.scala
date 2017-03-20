package org.apache.spark.ml.feature

import scala.math.round
import scala.collection.mutable.HashMap

class CorrelationsMatrix {

  val data = HashMap.empty[(Int,Int), Double]

  def apply(i:Int, j:Int): Double = {
    
    require(i != j, 
      "Correlations of a feature with itself shouldn't be calculated")
    
    // Only pairs (i,j) where i < j are stored
    if(i < j) data((i,j)) else data((j,i))
  }

  // Add new correlations to matrix
  def update(newFeatsPairs: Seq[(Int,Int)], correlator: Correlator) = {
    newFeatsPairs.foreach{ case(i,j) => 
      data((i,j)) = correlator.correlate(i,j)
    }
  }

  def keys: Seq[(Int, Int)] = data.keysIterator.toSeq

  def isEmpty: Boolean = data.isEmpty

  override def toString(): String =  {
    this.keys.sorted
      .map{ pair => s"$pair = ${data(pair)}" }.mkString("\n")
  }

  // TODO TEMP
  def toStringCorrsWithClass(iClass: Int): String = {
    (0 until iClass).map( i=> s"${data((i, iClass))}" ).mkString(",")
  } 
 
}