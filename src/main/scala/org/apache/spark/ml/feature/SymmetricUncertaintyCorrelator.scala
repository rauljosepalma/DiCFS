package org.apache.spark.ml.feature

import scala.math.log
import scala.collection.mutable

// sameFeature represents default value for two indentical features
abstract class Correlator(val sameFeatureValue: Double) {
  def correlate(featA: Int, featB: Int): Double
}


class SymmetricUncertaintyCorrelator(
  ctm: ContingencyTablesMatrix, nInstances: Long,
  previousEntropies: IndexedSeq[Double], iClass: Int) 
  extends Correlator(sameFeatureValue=1.0) with Serializable {

  // The smallest deviation allowed in double comparisons. (from WEKA)
  val SMALL: Double = 1e-6

  // Entropies are frequently needed by the method correlate,
  // so they are calculated and cached
  val entropies: IndexedSeq[Double] = 
    if(previousEntropies.isEmpty)
      ctm.featsValuesCounts.map{ valuesCounts: mutable.Map[Double, Int] =>
        (valuesCounts
          .map{ case (_, count) => count * log(count) }
          // Using nInstances assumes that there are no missing values
          .sum * (-1.0/nInstances) + log(nInstances)
        )
      }
    else
      previousEntropies

  //DEBUG
  // println("ENTROPIES=")
  // entropies.foreach(println)

  // Since conditionalEntropies should be asked once, they are not stored to
  // prevent reserving memory that will be needed for the correlations matrix.
  private def conditionalEntropy(iConditionedFeat: Int, iFeat: Int): Double = {
    
    ctm.tables(iConditionedFeat, iFeat)
      .map{ case (_, count) => count * log(count) }
      .sum * (-1.0/nInstances) - entropies(iFeat) + log(nInstances)

  }

  private def approxEq(a: Double, b: Double): Boolean = {
    ((a == b) || ((a - b < SMALL) && (b - a < SMALL)))
  }

  override def correlate(iFeatA: Int, iFeatB: Int): Double = {
    // This to alert conditionalEntropies being calculated unnecessarly i.e.
    // if conditionalEntropy(1,5) was calculated then there is no need to
    // calculate conditionalEntropy(5,1)
    require(iFeatA < iFeatB, "iFeatA must always be less than iFeatB")
    
    val infoGain = entropies(iFeatA) - conditionalEntropy(iFeatA, iFeatB)
    val denom = entropies(iFeatA) + entropies(iFeatB)
    
    // Two feats with a single value will have both zero entropies
    // and consecuently a denom == 0
    val correlation = if (denom == 0.0) 0.0 else (2.0 * infoGain / denom)
    
    // This condition was taken from WEKA source code     
    // If two classes have approximately zero correlation but neither of them
    // is the class, then their SymmetricUncertainty is considered to be 1.0
    if(approxEq(correlation,0.0)){
      if(iFeatA == iClass || iFeatB == iClass) 0.0 else 1.0
    } else {
      correlation
    }
  }

}