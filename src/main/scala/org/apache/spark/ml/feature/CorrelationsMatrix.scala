package org.apache.spark.ml.feature

import scala.math.round
import scala.collection.mutable.HashMap
import scala.collection.immutable.BitSet

// nFeats includes class
class CorrelationsMatrix(val nFeats: Int) {

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
      require(i < j, "In a featPair(i,j) i must always be less than j")

      data((i,j)) = correlator.correlate(i,j)
    }
  }

  // Clean corrs not in remainingFeats except corrs with class
  def clean(remainingFeats: Seq[Int]) = {

    val remainingFeatsSet = BitSet(remainingFeats:_*)
    data.retain{ case ((iFeatA: Int, iFeatB: Int), _) =>
      ((remainingFeatsSet.contains(iFeatA) && 
        remainingFeatsSet.contains(iFeatB)) 
        || iFeatB == nFeats)
    } 
  }

  def keys: Seq[(Int, Int)] = data.keysIterator.toSeq

  def isEmpty: Boolean = data.isEmpty

  override def toString(): String =  {
    this.keys.sorted
      .map{ pair => s"$pair = ${data(pair)}" }.mkString("\n")
  }

  // TODO TEMP
  def toStringCorrsWithClass: String = {
    (0 until nFeats).map( i=> s"${data((i, nFeats))}" ).mkString(",")
  } 
 
}