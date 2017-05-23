package org.apache.spark.ml.feature

import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD

import breeze.linalg.DenseMatrix

// sameFeature represents default value for two indentical features
abstract class Correlator(val sameFeatureValue: Double) extends Serializable {
  def correlate(pairs: Seq[(Int,Int)]): Map[(Int,Int),Double]
}

// attrs must contain Attribute objects for all feats including the class and
// the end
class SUCorrelator(
  rdd: RDD[Array[Byte]], attrsSizes: IndexedSeq[Int])
  extends Correlator(sameFeatureValue=1.0) {

  var totalPairsEvaluated = 0
  
  // By convention it is considered that class is stored after the last
  // feature in the correlations matrix
  private val iClass = attrsSizes.size - 1
  // The smallest deviation allowed in double comparisons. (from WEKA)
  private val SMALL: Double = 1e-6

  // TODO Check is useful to cache entropies
  private var entropies = IndexedSeq.empty[Double]
  // Broadcasted sizes for ctables creation
  private val bCtSizes = rdd.context.broadcast(attrsSizes)

  // ContingencyTables 
  private var ctables: RDD[((Int,Int), ContingencyTable)] = _

  private def calculateCTables(pairs: Seq[(Int,Int)]) = {
    val bPairs = rdd.context.broadcast(pairs)

    ctables = rdd.mapPartitions{ partition =>
      val rows: Array[Array[Byte]] = partition.toArray
      bPairs.value.map{ case (i,j) =>
        val m = DenseMatrix.zeros[Double](bCtSizes.value(i), bCtSizes.value(j))
        rows.foreach{ row => m(row(i),row(j)) += 1.0 }
        ((i,j), new ContingencyTable(m))
      }.toIterator
    }.reduceByKey(_ + _)
  }
  
  // Calculates entropies for all feats including the class,
  // This method only works if ctables for all feats vs the class exist.
  private def calculateEntropies: IndexedSeq[Double] = {
    val featsEntropies: IndexedSeq[Double] = 
      ctables
        .map{ case ((i,j), ct) => (i, ct.rowsEntropy) } 
        .collect.sortBy(_._1).map(_._2)
    val classEntropy: Double = ctables.first._2.colsEntropy

    featsEntropies :+ classEntropy
  }
 
  override def correlate(pairs: Seq[(Int,Int)])
    : Map[(Int,Int),Double] = {

    require(!pairs.isEmpty, 
      "Cannot create ContingencyTables with empty pairs collection")

    // DEBUG
    println(s"EVALUATING ${pairs.size} PAIRS:")
    println(pairs.mkString(","))
    // val difFeats = t.flatMap(p => Seq(p._1,p._2)).distinct.size
    totalPairsEvaluated += pairs.size

    // Hard work!
    calculateCTables(pairs)
    
    if(entropies.isEmpty) {
      entropies = calculateEntropies
      // DEBUG
      // println("ENTROPIES = ")
      // println(entropies.mkString("\n"))
    }
    // TODO test if this is better than calculating entropies again
    val bEntropies = ctables.context.broadcast(entropies)
    
    ctables.map{ case ((i,j), ct) => 

      // val entropyI = ct.rowsEntropy
      // val entropyJ = ct.colsEntropy
      // val infoGain = entropyI - ct.condEntropy(entropyJ) 
      // val denom = entropyI + entropyJ
      val infoGain = bEntropies.value(i) - ct.condEntropy(bEntropies.value(j)) 
      val denom = bEntropies.value(i) + bEntropies.value(j)
      
      // Two feats with a single value will have both zero entropies
      // and consecuently a denom == 0
      val correlation = if (denom == 0.0) 0.0 else (2.0 * infoGain / denom)
      
      // This condition was taken from WEKA source code     
      // If two classes have approximately zero correlation but neither of them
      // is the class, then their SymmetricUncertainty is considered to be 1.0
      if(approxEq(correlation,0.0))
        if(i == iClass || j == iClass) ((i,j),0.0) else ((i,j),1.0)
      else
        ((i,j),correlation)
    }.collectAsMap().toMap
  }

  private def approxEq(a: Double, b: Double): Boolean = {
    ((a == b) || ((a - b < SMALL) && (b - a < SMALL)))
  }
}