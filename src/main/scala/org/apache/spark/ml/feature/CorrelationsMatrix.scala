package org.apache.spark.ml.feature

import scala.math.round
import scala.collection.mutable

// nFeats includes class
class CorrelationsMatrix(correlator: Correlator) {

  private val data = mutable.Map.empty[(Int,Int), Double]

  def precalcCorrs(pairs: Seq[(Int,Int)]): Unit = {
    pairs.foreach{ case (i,j) =>
      require(i < j, s"In a featPair(i=$i,j=$j) i must always be less than j")
    }
    require(pairs.distinct.size == pairs.size, 
      "All required pairs must be different")

    val newPairs = pairs.filter(!data.contains(_))
    // The hard work line!
    val newPairsCorrs = 
      if(!newPairs.isEmpty) correlator.correlate(newPairs)
      else Seq.empty[Double]
    // Update data
    newPairs.zip(newPairsCorrs).foreach{ case(pair,corr) => data(pair) = corr }
  }

  def apply(i: Int, j: Int): Double = data(i,j)
  def apply(pair: (Int,Int)): Double = data(pair)
  // Remove corrs not in pairs, pairs should include pairs with class
  def clean(pairs: Seq[(Int,Int)]): CorrelationsMatrix = {
    data.retain{ case (pair,_) => pairs.contains(pair) }
    this
  }

  def size: Int = data.size

  override def toString(): String =  {
    this.data.keys.toSeq.sorted
      .map{ pair => s"$pair = ${data(pair)}" }.mkString("\n")
  }

  // TODO TEMP
  // def toStringCorrsWithClass: String = {
  //   (0 until nFeats).map( i=> s"${data((i, nFeats))}" ).mkString(",")
  // } 
 
}