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

  override def preEvaluate(states: Seq[EvaluableState]): Unit = {
    // TODO Run-time check was the only solution found
    preEvaluateFS(states.map{ 
        case s:FeaturesSubset => s 
        case _ => throw new IllegalArgumentException
      })
  }
  override def evaluate(states: Seq[EvaluableState]): Seq[EvaluatedState] = {
    evaluateFS(states.map{ 
        case s:FeaturesSubset => s 
        case _ => throw new IllegalArgumentException
      })
  }


  def preEvaluateFS(states: Seq[FeaturesSubset]): Unit = {
    // Precalc all feats pairs (if needed) using corrs
    val allPairs: Seq[(Int,Int)] = {
      val allFeats: FeaturesSubset = states.reduce(_ ++ _)
      val allFeatsWithClass: Seq[(Int,Int)] = 
        allFeats.getPairsWithFeat(iClass)
      val interFeatPairs: Seq[(Int,Int)] = 
        states.flatMap(_.getInterFeatPairs).distinct

      allFeatsWithClass ++ interFeatPairs
    }
    // The hard-work!
    corrs.precalcCorrs(allPairs)
  }

  // Evals a given subset of features
  // Empty states are assigned with 0 correlation
  def evaluateFS(states: Seq[FeaturesSubset]): Seq[EvaluatedState] = {

    states.map{ state =>
      
      numOfEvaluations += 1

      val numerator = state.getPairsWithFeat(iClass).map(corrs(_)).sum
      val interFeatCorrelations = state.getInterFeatPairs.map(corrs(_)).sum
      val denominator = sqrt(state.size + 2.0 * interFeatCorrelations)

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
        
      new EvaluatedState(state, merit)
    }
  }
}