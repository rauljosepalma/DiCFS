package org.apache.spark.ml.feature

import scala.math.sqrt

class CfsSubsetEvaluator(corrs: CorrelationsMatrix, iClass: Int)
  extends StateEvaluator {

  // TODO 
  // Caching was disabled since, in the case of distributed execution,
  // WeakHashMaps could not be serialized and because, even if it could,
  // sending serialized cache could be slower than calculating it again.
  // However, in the case of local execution previous tests with WEKA showed no
  // benefits of using it in terms of execution time.

  // A WeakHashMap does not creates strong references, so its elements
  // can be garbage collected if there are no other references to it than this,
  // in the case of BestFirstSearch, the subsets are stored in the queue
  // var cache: WeakHashMap[BitSet, Double] = WeakHashMap[BitSet,Double]()

  var numOfEvaluations = 0
  var numOfPasses = 0

  override def preEvaluate(states: Seq[EvaluableState]): Unit = {
    // TODO Run-time check was the only solution found
    preEvaluateFS(states.map{ 
        case s:FeaturesSubset => s 
        case _ => throw new IllegalArgumentException
      })
  }
  override def evaluate(state: EvaluableState): EvaluatedState = {
    // TODO Run-time check was the only solution found
    evaluateFS(state.asInstanceOf[FeaturesSubset])
  }

  // This method prefills the corrs matrix before the evaluation.
  // It leverages the following facts:
  // - All subsets have the same size
  // - Any subset contains on its penultimate element the newest feature added
  // - Only correlations between the newest feature and the features in the
  //   last position (added by expand) COULD BE missing in corrs matrix.
  // - In the case of feature that was evaluated and expanded and then not 
  //   added (causing a fail), it is possible that some of the partners sent
  //   have already been evaluated.
  def preEvaluateFS(subsets: Seq[FeaturesSubset]): Unit = {

    val (iFeat, partners): (Int, Seq[Int]) = 
      if(subsets.head.size > 1) {
        (subsets.head.getPenultimateFeat, subsets.map(_.getLastFeat))
      // The first time, when the subsets contain all single features
      // they must be compared with the class
      } else {
        (iClass, Range(0, iClass))
      }

    // The hard work!
    corrs.precalcCorrs(iFeat, partners)
    if (!partners.isEmpty) { numOfPasses += 1 }
      
  }

  // Evals a given subset of features
  // Empty states are assigned with 0 correlation
  def evaluateFS(subset: FeaturesSubset): EvaluatedState = {
      
    numOfEvaluations += 1

    val numerator = subset.getPairsWithClass(iClass).map(corrs(_)).sum
    val interFeatCorrelations = subset.getInterFeatPairs.map(corrs(_)).sum
    val denominator = sqrt(subset.size + 2.0 * interFeatCorrelations)

    // Take care of aproximations problems
    val merit = 
      if (denominator == 0.0) {
        0.0
      } else {
        if (numerator/denominator < 0.0) {
          -numerator/denominator
        } else {
          numerator/denominator
        }
      }
      
    new EvaluatedState(subset, merit)
  }
}