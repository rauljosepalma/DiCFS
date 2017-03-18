package org.apache.spark.ml.feature

import scala.math.sqrt
import scala.collection.immutable.BitSet

// The option parameters are needed because the evaluator can run on the driver
// or on the workers.
class CfsSubsetEvaluator(correlations: CorrelationsMatrix, iClass: Int)
  extends StateEvaluator[BitSet] {

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

  // Evals a given subset of features
  override def evaluate(state: EvaluableState[BitSet]): 
    Double = {

    val subset: BitSet = state.data

    // if(cache.contains(subset)) {
    //   cache(subset)
    // } else {

    numOfEvaluations += 1

    val numerator = subset.map(correlations(_,iClass)).sum
    val interFeatCorrelations = 
      subset.toSeq.combinations(2)
        .map{ e => correlations(e(0), e(1)) }.sum

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
      
    // cache(subset) = merit
      
    merit
    
    // }
  }
}