package org.apache.spark.ml.feature

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import scala.collection.immutable.BitSet
// import scala.collection.mutable.WeakHashMap

class CfsFeatureSelector {

  // Searches a subset of features given the data
  // Returns a BitSet containing the selected features
  // df DataFrame must be cached and discretized
  def fit(
    df: DataFrame,
    debugFileBasePath: String,
    addLocalFeats:Boolean, 
    // partitionSize: Int,


    useGAOptimizer: Boolean, 
    useNFeatsForPopulationSize: Boolean, 
    optIslandPopulationSize: Int): BitSet = {

    val nFeats = df.take(1)(0)(0).asInstanceOf[Vector].size
    val nInstances: Long = df.count
    // By convention it is considered that class is stored after the last
    // feature in the correlations matrix
    val iClass = nFeats
    // The number of feats must include the class (nFeats + 1)
    val contingencyTablesMatrix = 
      ContingencyTablesMatrix(df, nFeats + 1)
    // The number of feats must include the class (nFeats + 1)
    val correlations = CorrelationsMatrix(
      new SymmetricUncertaintyCorrelator(
        contingencyTablesMatrix, nInstances), nFeats + 1)

    // DEBUG
    var file = new java.io.FileWriter(
      s"${debugFileBasePath}_correlations.txt", true)
    file.write(correlations.toString)
    file.close


    // DEBUG
    // println("CORRELATIONS MATRIX=")
    // (0 to nFeats).map {
    //   fA => {
    //     println( (0 to nFeats).map(fB => "%1.4f".format(correlations(fA,fB))) )
    //   }
    // }

    val subsetEvaluator = if(useGAOptimizer) {
      // In this case the CorrelationsMatrix is broadcasted
      val correlationsBC = df.sparkSession.sparkContext.broadcast(correlations)
      new CfsSubsetEvaluator(Some(correlationsBC), None)
    } else {
      // In this case the CorrelationsMatrix is local to the driver
      new CfsSubsetEvaluator(None, Some(correlations))
    }

    val optimizer = if(useGAOptimizer) {

      val nIslands = df.sparkSession.sparkContext.defaultParallelism

      val islandPopulationSize: Int = 
        if(useNFeatsForPopulationSize) {
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

    val result: EvaluatedState[BitSet] = optimizer.search
    val subset: BitSet = result.state.data
    
    //DEBUG
    println("NUMBER OF EVALUATIONS=" + subsetEvaluator.numOfEvaluations)
    println("BEST SUBSET = " + result.toString)
    
    // Add locally predictive feats is requested
    if (addLocalFeats) {

      // Receives the actual subset of selected feats and the remaning feats
      // in descending order according to their correlation to the class
      def addLocallyPredictiveFeats(
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
            // DEBUG
            println(s"ADDING LOCALLY PREDICTIVE FEAT: $candFeat")
            addLocallyPredictiveFeats(
              selectedFeats + candFeat, orderedCandFeats.tail)
          // Ignore feat
          } else {
            addLocallyPredictiveFeats(selectedFeats, orderedCandFeats.tail)
          }
        }
      }

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

}