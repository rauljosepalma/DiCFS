package org.apache.spark.ml.feature

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint

import scala.collection.immutable.BitSet
import scala.collection.mutable.WeakHashMap
import scala.math.sqrt


class CfsSubsetEvaluator(correlations: CorrelationsMatrix) 
  extends StateEvaluator[BitSet] {

  // A WeakHashMap does not creates strong references, so its elements
  // can be garbage collected if there are no other references to it than this,
  // in the case of BestFirstSearch, the subsets are stored in the queue
  var cache: WeakHashMap[BitSet, Double] =
    WeakHashMap()

  var numOfEvaluations = 0

  // Evals a given subset of features
  override def evaluate(state: EvaluableState[BitSet]): 
    Double = {


    val subset: BitSet = state.evaluableData

    if(cache.contains(subset)){
      cache(subset)
    }else{
      numOfEvaluations += 1
      // We are applying a simplified version of the heuristic formula
      val iClass = correlations.nFeats - 1
      val numerator = subset.map(correlations(_,iClass)).sum
      val interFeatCorrelations = 
        subset.toSeq.combinations(2)
          .map{ e => correlations(e(0), e(1)) }.sum

      val denominator = sqrt(subset.size + 2.0 * interFeatCorrelations)

      // TODO Check if this is really needed
      // Take care of aproximations problems and return EvaluatedState
      // if (denominator == 0.0) {
      //   new EvaluatedState(state, Double.NegativeInfinity)
      // } else if (numerator/denominator < 0.0) {
      //   new EvaluatedState(state, -numerator/denominator)
      // } else {
      //   new EvaluatedState(state, numerator/denominator)
      // }
      
      val merit = numerator/denominator
      cache(subset) = merit
      
      merit
    }
  }
}

// data must be cached and discretized
class CfsFeatureSelector(data: RDD[LabeledPoint]) {

  private val nFeats = data.take(1)(0).features.size

  private val nInstances: Long = data.count

  // By convention it is considered that class is stored after the last feature
  // in the correlations matrix
  private val iClass = nFeats

  // The number of feats must include the class (nFeats + 1)
  private val contingencyTablesMatrix = 
    ContingencyTablesMatrix(data, nFeats + 1)

  // The number of feats must include the class (nFeats + 1)
  private val correlations = new CorrelationsMatrix(
    new SymmetricUncertaintyCorrelator(contingencyTablesMatrix, nInstances), nFeats + 1)

  // Searches a subset of features given the data
  // Returns a BitSet containing the selected features
  def searchFeaturesSubset(
    addLocalFeats:Boolean, useGAOptimizer: Boolean): BitSet = {

    // DEBUG
    // println("CORRELATIONS MATRIX=")
    // (0 to nFeats).map {
    //   fA => {
    //     println( (0 to nFeats).map(fB => "%1.4f".format(correlations(fA,fB))) )
    //   }
    // }

    val subsetEvaluator = new CfsSubsetEvaluator(correlations)

    val optimizer = if(useGAOptimizer) {
      // TODO! CorrelationsMatrix mus be broadcasted in the GA case!!

      // DEBUG
      println("Using a GA Optimizer")
      val islandPopulationSize = 20
      val nIslands = data.context.defaultParallelism
      new GAOptimizer[BitSet](
        evaluator = subsetEvaluator,
        mutator = new FeaturesSubsetMutator,
        matchmaker = new UniformFeaturesSubsetMatchmaker,
        populationGenerator = 
          (new FeaturesSubsetPopulationGenerator(
            islandPopulationSize = islandPopulationSize,
            nIslands = nIslands,
            partitioner = 
              (new RandomPartitioner(
                numParts = data.context.defaultParallelism, 
                numElems = nIslands * islandPopulationSize)
              ),
            nFeats = nFeats,
            sc = data.context)
          ),
        maxGenerations = 30,
        maxTimeHours = 0.25,
        minMerit = Double.PositiveInfinity
      )
  
    } else { 
      // DEBUG
      println("Using a Best First Searcher")
      new BestFirstSearcher(
        initialState = new FeaturesSubset(BitSet(), nFeats),
        evaluator = subsetEvaluator,
        maxFails = 4)
    }

    val result: EvaluableState[BitSet] = optimizer.search
    val subset: BitSet = result.evaluableData
    val merit: Double = subsetEvaluator.evaluate(result)
    // DEBUG
    println("BEST MERIT = " + merit)

    //DEBUG
    println("NUMBER OF EVALUATIONS=" + subsetEvaluator.numOfEvaluations)

    
    // Add locally predictive feats is requested
    if (addLocalFeats) {
      // Descending ordered remaning feats according to their correlation with
      // the class
      val remaningFeats: Seq[Int] = 
        (0 until nFeats)
          .filter(!subset.contains(_))
          .sortWith{
            (a, b) => correlations(a, iClass) > correlations(b, iClass)}
      
      addLocallyPredictiveFeats(subset, remaningFeats)

    // Do not add locally predictive feats
    } else {
      subset
    }
  }

  // Receives the actual subset of selected feats and the remaning feats in
  // descending order according to their correlation to the class
  private def addLocallyPredictiveFeats(
    selectedFeats: BitSet, 
    orderedCandFeats: Seq[Int]): BitSet = {

    if (orderedCandFeats.isEmpty){
      selectedFeats
    } else {
      // Check if next candidate feat is more correlated to the class than
      // to any of the selected feats
      val candFeat = orderedCandFeats.head
      val candFeatClassCorr = correlations(candFeat,iClass)
      val tempSubset = 
        selectedFeats
          .filter { f => (correlations(f,candFeat) > candFeatClassCorr) }
      // Add feat to the selected set
      if(tempSubset.isEmpty){
        addLocallyPredictiveFeats(
          selectedFeats + candFeat, orderedCandFeats.tail)
      // Ignore feat
      } else {
        addLocallyPredictiveFeats(selectedFeats, orderedCandFeats.tail)
      }
    }
  }
}