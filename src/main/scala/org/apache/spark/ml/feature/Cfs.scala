package org.apache.spark.ml.feature

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast

import scala.collection.immutable.BitSet
// import scala.collection.mutable.WeakHashMap
import scala.math.sqrt

// The option parameters are needed because the evaluator can run on the driver
// or on the workers.
class CfsSubsetEvaluator(
  correlationsBC: Option[Broadcast[CorrelationsMatrix]], 
  correlationsMX: Option[CorrelationsMatrix])
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
    
    val correlations = 
      correlationsMX match { 
        case Some(cm: CorrelationsMatrix) => cm
        case None => correlationsBC match {
          case Some(bc: Broadcast[CorrelationsMatrix]) => bc.value
          case None => throw new SparkException(
            "CfsSubsetEvaluator must receive a correlations matrix");
        }
      }

    val iClass = correlations.nFeats - 1
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
  private val correlations = CorrelationsMatrix(
    new SymmetricUncertaintyCorrelator(contingencyTablesMatrix, nInstances), nFeats + 1)

  // Searches a subset of features given the data
  // Returns a BitSet containing the selected features
  def searchFeaturesSubset(
    addLocalFeats:Boolean, useGAOptimizer: Boolean, usePopGTEnFeats: Boolean, optIslandPopulationSize: Int): BitSet = {

    // DEBUG
    // println("CORRELATIONS MATRIX=")
    // (0 to nFeats).map {
    //   fA => {
    //     println( (0 to nFeats).map(fB => "%1.4f".format(correlations(fA,fB))) )
    //   }
    // }

    val subsetEvaluator = if(useGAOptimizer) {
      // In this case the CorrelationsMatrix is broadcasted
      val correlationsBC = data.context.broadcast(correlations)
      new CfsSubsetEvaluator(Some(correlationsBC), None)
    } else {
      // In this case the CorrelationsMatrix is local to the driver
      new CfsSubsetEvaluator(None, Some(correlations))
    }

    val optimizer = if(useGAOptimizer) {


      val nIslands = data.context.defaultParallelism

      val islandPopulationSize: Int = 
        if(usePopGTEnFeats) {
          if ((nFeats/nIslands) % 2 != 0) 
            (nFeats/nIslands) + 1
          else (nFeats/nIslands)
        } else optIslandPopulationSize

      val eliteSize: Int = {
        val temp = (islandPopulationSize*0.10).toInt
        if (temp % 2 == 0) temp else temp + 1
      }

      // DEBUG
      println("Using a GA Optimizer")
      println(s"nIslands = $nIslands")
      println(s"islandPopulationSize = $islandPopulationSize")
      println(s"eliteSize = $eliteSize")

      new GAOptimizer[BitSet](
        mutator = new FeaturesSubsetMutator,
        matchmaker = new UniformFeaturesSubsetMatchmaker,
        populationGenerator = 
          (new FeaturesSubsetPopulationGenerator(
            islandPopulationSize = islandPopulationSize,
            nIslands = nIslands,
            evaluator = subsetEvaluator,
            partitioner = 
              (new RandomPartitioner(
                numParts = nIslands, 
                numElems = nIslands * islandPopulationSize)
              ),
            nFeats = nFeats)
          ),
        maxGenerations = 30,
        maxTimeHours = 0.25,
        minMerit = Double.PositiveInfinity,
        eliteSize =  eliteSize,
        tournamentSize = 2
      )
  
    } else { 

      // DEBUG
      println("Using a Best First Searcher")

      new BestFirstSearcher(
        initialState = new FeaturesSubset(BitSet(), nFeats),
        evaluator = subsetEvaluator,
        maxFails = 5)
      
    }

    //DEBUG
    println("NUMBER OF EVALUATIONS=" + subsetEvaluator.numOfEvaluations)

    val result: EvaluatedState[BitSet] = optimizer.search
    val subset: BitSet = result.state.data
    
    // DEBUG
    println("BEST SUBSET = " + result.toString)
    
    // Add locally predictive feats is requested
    if (addLocalFeats) {
      // DEBUG
      println("ADDING LOCALLY PREDICTIVE FEATS...")
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