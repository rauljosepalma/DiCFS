package org.apache.spark.ml.feature

import scala.math.round
import scala.collection.mutable

// nFeats includes class
class CorrelationsMatrix(correlator: Correlator) {

  private val corrs = mutable.Map.empty[(Int,Int), Double]

  def precalcCorrs(pairs: Seq[(Int,Int)]): Unit = {
    pairs.foreach{ case (i,j) =>
      require(i < j, s"In a featPair(i=$i,j=$j) i must always be less than j")
    }
    require(pairs.distinct.size == pairs.size, 
      "All required pairs must be different")

    val newPairs = pairs.filter(!corrs.contains(_))
    // The hard work line!
    val newPairsCorrs: Map[(Int,Int), Double] = 
      if(!newPairs.isEmpty) correlator.correlate(newPairs)
      else Map.empty[(Int,Int), Double] 
    // Update corrs
    newPairsCorrs.foreach{ case (pair,corr) => corrs(pair)=corr }
  }

  def apply(i: Int, j: Int): Double = corrs(i,j)
  def apply(pair: (Int,Int)): Double = corrs(pair)
  // Remove corrs not in pairs, pairs should include pairs with class
  def clean(pairs: Seq[(Int,Int)]): CorrelationsMatrix = {
    corrs.retain{ case (pair,_) => pairs.contains(pair) }
    this
  }

  def size: Int = corrs.size

  override def toString(): String =  {
    this.corrs.keys.toSeq.sorted
      .map{ pair => s"$pair = ${corrs(pair)}" }.mkString("\n")
  }
}