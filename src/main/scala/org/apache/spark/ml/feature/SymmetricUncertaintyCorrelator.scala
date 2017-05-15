package org.apache.spark.ml.feature

import scala.collection.mutable

import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD

// sameFeature represents default value for two indentical features
abstract class Correlator(val sameFeatureValue: Double) {
  def correlate(pairs: Seq[(Int,Int)]): Seq[Double]
}


class SymmetricUncertaintyCorrelator(rdd: RDD[Row], nFeats: Int)
  extends Correlator(sameFeatureValue=1.0) with Serializable {

  // By convention it is considered that class is stored after the last
  // feature in the correlations matrix
  val iClass = nFeats
  // The smallest deviation allowed in double comparisons. (from WEKA)
  val SMALL: Double = 1e-6
  // Cache entropies
  // Since conditionalEntropies should be asked once, they are not stored to
  // prevent reserving memory that will be needed for the correlations matrix.
  val entropies: Array[Double] = Array.fill(nFeats+1)(-1.0)
  // Assuming there are no missing values
  var nInstances: Int = _

  // TODO DELETE?
  // val entropies: IndexedSeq[Double] = 
  //   ctm.featsValuesCounts.map{ valuesCounts: mutable.Map[Double, Int] =>
  //     (valuesCounts
  //       .map{ case (_, count) => count * log(count) }
  //       // Using nInstances assumes that there are no missing values
  //       .sum * (-1.0/nInstances) + log(nInstances)
  //     )
  //   }

  //DEBUG
  // println("ENTROPIES=")
  // entropies.foreach(println)

  private def approxEq(a: Double, b: Double): Boolean = {
    ((a == b) || ((a - b < SMALL) && (b - a < SMALL)))
  }
  
  override def correlate(pairs: Seq[(Int,Int)]): Seq[Double] = {
    
    // Hard work!
    val ctm = ContingencyTablesMatrix(rdd,pairs)
    
    // Calc entropies if needed
    pairs.foreach{ case(i,j) => 
      if(entropies(i) == -1.0) { 
        val (entropy, totalDistinctValues) = 
          ctm(i,j).calcEntropyAndTotalDistinctValues(firstFeat=true)
        // Assuming there are no missing values, and all features share same
        // value
        nInstances = totalDistinctValues
        entropies(i) = entropy
      }
      if(entropies(j) == -1.0) 
        entropies(j) = 
          ctm(i,j).calcEntropyAndTotalDistinctValues(firstFeat=false)._1
    }

    pairs.map{ case(i,j) =>
      val infoGain = entropies(i) - 
        ctm(i,j).calcCondEntropy(entropies(j), nInstances)
      val denom = entropies(i) + entropies(j)
      
      // Two feats with a single value will have both zero entropies
      // and consecuently a denom == 0
      val correlation = if (denom == 0.0) 0.0 else (2.0 * infoGain / denom)
      
      // This condition was taken from WEKA source code     
      // If two classes have approximately zero correlation but neither of them
      // is the class, then their SymmetricUncertainty is considered to be 1.0
      if(approxEq(correlation,0.0))
        if(i == iClass || j == iClass) 0.0 else 1.0
      else
        correlation
    }
  }

}