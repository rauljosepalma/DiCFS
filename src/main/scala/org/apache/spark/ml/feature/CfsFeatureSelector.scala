package org.apache.spark.ml.feature

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import scala.collection.immutable.BitSet

class CfsFeatureSelector {

  // Searches a subset of features given the data
  // Returns a BitSet containing the selected features
  // df DataFrame must be discretized
  // If initialPartitionSize == nFeats in df, then no partitioning is perfomed
  def fit(
    df: DataFrame,
    resultsFileBasePath: String,
    addLocalFeats: Boolean, 
    maxFails: Int,
    initialPartitionSize: Int): BitSet = {

    val nFeats = df.take(1)(0)(0).asInstanceOf[Vector].size
    val nInstances: Long = df.count
    // By convention it is considered that class is stored after the last
    // feature in the correlations matrix
    val iClass = nFeats

    require(initialPartitionSize == nFeats || initialPartitionSize <= nFeats/2,
      "Partition size must be less than or equal to half size of the total number of feats or it must be equal to it")

    // remainingFeats are not necessarly in order (140,141,142, etc)
    def findSubset(
      currentPartitionSize: Int,
      remainingFeats: BitSet, 
      entropies: IndexedSeq[Double],
      correlations: CorrelationsMatrix,
      initialFeatsPairsCount: Int=0): BitSet = {

      // The first time this function is called correlations matrix is empty
      val isFirstTime = correlations.isEmpty

      // Clean unneeded correlations (keep corrs with class)
      if (!isFirstTime) correlations.clean(remainingFeats)

      // Try to increase the partition size so the number of pair correlations
      // calculated is the maximum allowed by the initialPartitionSize
      val adjustedPartitionSize = {
        if (isFirstTime) initialPartitionSize
        else getAdjustedPartitionSize(
          initialFeatsPairsCount,
          currentPartitionSize,
          remainingFeats,
          correlations)
      }

      val (partitions: Seq[BitSet], remainingFeatsPairs: Seq[(Int,Int)]) = 
        getPartitionsAndRemainingFeatsPairs(
          adjustedPartitionSize,
          remainingFeats,
          correlations)

      val newInitialFeatsPairsCount = 
        if (isFirstTime) remainingFeatsPairs.size else initialFeatsPairsCount

      // DEBUG
      println(s"PARTITION SIZE=$adjustedPartitionSize")
      println(s"ABOUT TO PROCESS ${remainingFeatsPairs.size} FEATS PAIRS...")

      // DEBUG TEST
      val pairsCount = countRemainingFeatsPairs(
        adjustedPartitionSize,
        remainingFeats,
        correlations)

      require(pairsCount == remainingFeatsPairs.size, 
        s"ERROR: pairsCount != remainingFeatsPairs.size ($pairsCount != ${remainingFeatsPairs.size})")

      val ctm = ContingencyTablesMatrix(
        df, 
        if (isFirstTime) remainingFeats + iClass else remainingFeats,
        remainingFeatsPairs,
        precalcEntropies=isFirstTime)

      val correlator = new SymmetricUncertaintyCorrelator(
        ctm, nInstances, entropies, iClass)

      // Update correlations matrix
      correlations.update(remainingFeatsPairs, correlator)

      // DEBUG
      // println("partitions:")
      // println(partitions.mkString("\n"))
      // println("remainingFeats:")
      // println(remainingFeats.mkString(" "))
      // println("remainingFeatsPairs:")
      // println(remainingFeatsPairs.mkString(" "))
      if(isFirstTime){
        println("Saving corrs with class...")
        var file = new java.io.FileWriter(s"${resultsFileBasePath}_corrsWithClass.txt", true)
        file.write(correlations.toStringCorrsWithClass(iClass))
        file.close
      }

      val subsetEvaluator = new CfsSubsetEvaluator(correlations, iClass)

      // Search subsets on each partition, merge results.
      val newRemainingFeats: BitSet = 
        findAndMergeSubsetsInPartitions(partitions, subsetEvaluator, maxFails)

      //DEBUG
      println("NUMBER OF EVALUATIONS=" + subsetEvaluator.numOfEvaluations)
      println("BEST SUBSET=" + newRemainingFeats.toString)
      println("BEST SUBSET SIZE=" + newRemainingFeats.size.toString)
  
      // End when the features where not partitioned
      // Observe the adjustedPartitionSize warranties that the single partition
      // case will be always reached 
      if(partitions.size == 1){

        // Add locally predictive feats if requested
        if (!addLocalFeats)
          newRemainingFeats
        else 
          addLocallyPredictiveFeats(newRemainingFeats, correlations, iClass)

      } else {
        findSubset(
          adjustedPartitionSize, 
          newRemainingFeats, 
          correlator.entropies, 
          correlations,
          newInitialFeatsPairsCount)
      }
    }

    // DEBUG tempsubset
    val tempsubset = findSubset(
      currentPartitionSize = initialPartitionSize,
      remainingFeats = BitSet(Range(0, nFeats):_*), 
      entropies = IndexedSeq(),
      correlations = new CorrelationsMatrix(nFeats))

    // DEBUG
    var file = new java.io.FileWriter(
      s"${resultsFileBasePath}_selectedFeats.txt", true)
    file.write(tempsubset.mkString(","))
    file.close

    // DEBUG
    tempsubset

  }

  private def getPartitionsAndRemainingFeatsPairs(
    partitionSize: Int, 
    remainingFeats: BitSet,
    correlations: CorrelationsMatrix): (Seq[BitSet], Seq[(Int,Int)]) = {

    val isFirstTime = correlations.isEmpty

    // If remaningFeats.size < partitionSize then nPartitions == 1
    val nPartitions: Int = {
      if (remainingFeats.size % partitionSize == 0)
        remainingFeats.size / partitionSize 
      else remainingFeats.size / partitionSize + 1
    }

    // Sequential splits of partitionSize from features Ex.: If
    // remainingFeats.size = 5, 2 size splits are: [[0,1],[2,3],[4]]
    val partitions: Seq[BitSet] = {
      (0 until nPartitions - 1).map{ i => 
        Range(i * partitionSize, (i+1) * partitionSize)
      } :+ 
      Range((nPartitions-1) * partitionSize, remainingFeats.size)
    // Partitions indexes must be mapped to the remainingFeats
    }.map{ p =>  BitSet(p.map(remainingFeats.toSeq):_*) }

    // Feats pairs whose correlations have not been evaluated, 
    // Its important to keep (i,j), where i < j
    val remainingFeatsPairs: Seq[(Int,Int)] = 
      // The first time partitions include the class and correlations
      // matrix is empty
      if(isFirstTime) {
        partitions.flatMap{ p => 
          (p + correlations.nFeats).toSeq.combinations(2).map{ 
            pair => (pair(0), pair(1)) }
        }
      } else { 
        partitions
          .flatMap( _.toSeq.combinations(2) )
          .map( pair => (pair(0),pair(1)) )
          .diff(correlations.keys)
      }
      // {
      //   partition.toSeq.combinations(2)
      //     .filter{ pair =>
      //       !correlations.keys.contains(pair(0), pair(1)) }
      //     .map{ pair => (pair(0), pair(1)) }
      // }
    

    (partitions, remainingFeatsPairs)
  }

  private def countRemainingFeatsPairs(
    partitionSize: Int, 
    remainingFeats: BitSet,
    correlations: CorrelationsMatrix): Int = {

    val (_, remainingFeatsPairs: Seq[(Int,Int)]) = 
      getPartitionsAndRemainingFeatsPairs(
        partitionSize,
        remainingFeats,
        correlations)

    remainingFeatsPairs.size

    // A formula based version didn't work because is not enough to substract
    // corrsWithClass from correlations to obtain the useful already calculated
    // correlations, this is because there could be correlations that were
    // calculated and right now are not useful because their feats fall in
    // diferent partitions but could be useful in future.

    // val addClass = if(correlations.isEmpty) 1 else 0
    // val nFirstPartitions = remainingFeats.size / partitionSize
    // val firstPartitionsSize = partitionSize + addClass
    // val lastPartitionSize = remainingFeats.size % partitionSize + addClass
    // val corrsWithClass = if(!correlations.isEmpty) correlations.nFeats else 0
    
    // nFirstPartitions * combinations(firstPartitionsSize, 2) +
    //   combinations(lastPartitionSize, 2) - 
    //   (correlations.keys.size - corrsWithClass)
  }

  private def getAdjustedPartitionSize(
    initialFeatsPairsCount: Int,
    currentPartitionSize: Int,
    remainingFeats: BitSet,
    correlations: CorrelationsMatrix): Int = {

    require(!correlations.isEmpty, 
      "This function should never be called in the first time evaluation")

    // The partition size will always grow, so if there are only two option
    // it will take the max (remainingFeats.size)
    if(currentPartitionSize >= remainingFeats.size - 1)
      remainingFeats.size
    else {
      // Get two points to estimate slope
      val minFeatsPairsCount = countRemainingFeatsPairs(
        partitionSize=currentPartitionSize, 
        remainingFeats,
        correlations)
      val nextFeatsPairsCount = countRemainingFeatsPairs(
        partitionSize=currentPartitionSize + 1, 
        remainingFeats,
        correlations)
      val slope: Double = (nextFeatsPairsCount - minFeatsPairsCount).toDouble 
       // / (currentPartitionSize + 1 - currentPartitionSize) => 1

      // Given that y is featsPairsCount, and x is partitionSize y = slope * x
      // + b, b is always 0. By solving for x we obtaint the
      // adjustedPartitionSize. That is, the partitionSize that corresponds to
      // the same featsPairsCount during the first partitioned subset search.
      val adjustedPartitionSize = (initialFeatsPairsCount / slope).toInt

      if (adjustedPartitionSize >= remainingFeats.size)
        remainingFeats.size
      // When the suggested size is in the second half, a half size is
      // preferred to do a more equilibrate processing and with less
      // calculations due to the smaller partition size
      else if (adjustedPartitionSize >= remainingFeats.size / 2)
        remainingFeats.size / 2
      else
        adjustedPartitionSize
    }
  }

  //   // TODO This could be more efficently calculated by using a formula
  //   val (_, initialRemainingFeatsPairs) = 
  //     getPartitionsAndRemainingFeatsPairs(
  //       initialPartitionSize,
  //       BitSet(Range(0, nFeats):_*),
  //       new CorrelationsMatrix, // Simulates isFirtTime
  //       nFeats) // iClass is only important when isFirstTime 

  //   def doGetAdjustedPartitionSize(adjustedPartitionSize: Int): Int = {

  //     val (_, remainingFeatsPairs) = 
  //       getPartitionsAndRemainingFeatsPairs(
  //         adjustedPartitionSize,
  //         remainingFeats,
  //         correlations,
  //         nFeats) 
  //     if(remainingFeatsPairs.size <= initialRemainingFeatsPairs.size)
  //       adjustedPartitionSize
  //     else
  //       doGetAdjustedPartitionSize(adjustedPartitionSize - 1)
  //   }

  //   // Start search with the maximum partition size
  //   doGetAdjustedPartitionSize(remainingFeats.size)
  // }

  private def findAndMergeSubsetsInPartitions(
    partitions: Seq[BitSet], 
    evaluator: CfsSubsetEvaluator,
    maxFails: Int): BitSet = {

    val mergedFeats: Seq[Int] = 
      partitions.flatMap{ partition => 
        val optimizer = 
          new BestFirstSearcher(
            initialState = new FeaturesSubset(BitSet(), partition),
            evaluator,
            maxFails)
        val result: EvaluatedState[BitSet] = optimizer.search

        //DEBUG
        if(partitions.size == 1){
          println(s"BEST MERIT= ${result.merit}")
        }
        
        result.state.data
      }

    BitSet(mergedFeats:_*)
  }

  private def addLocallyPredictiveFeats(
    selectedSubset: BitSet, 
    correlations: CorrelationsMatrix, 
    iClass: Int) : BitSet = {

    // Descending order remaining feats according to their correlation with
    // the class
    val orderedCandFeats: Seq[Int] = 
      Range(0, iClass)
        .filter(!selectedSubset.contains(_))
        .sortWith{
          (a, b) => correlations(a, iClass) > correlations(b, iClass)}

    def doAddFeats(
      extendedSubset: BitSet, orderedCandFeats: Seq[Int]): BitSet = {

      if (orderedCandFeats.isEmpty) {
        extendedSubset
      } else {
        // Check if next candidate feat is more correlated to the class than
        // to any of the selected feats
        val candFeat = orderedCandFeats.head
        val candFeatClassCorr = correlations(candFeat,iClass)
        val tempSubset = 
          extendedSubset
            .filter { f => (correlations(f,candFeat) > candFeatClassCorr) }
        // Add feat to the selected set
        if(tempSubset.isEmpty){
          // DEBUG
          println(s"ADDING LOCALLY PREDICTIVE FEAT: $candFeat")
          doAddFeats(
            extendedSubset + candFeat, orderedCandFeats.tail)
        // Ignore feat
        } else {
          doAddFeats(extendedSubset, orderedCandFeats.tail)
        }
      }
    }

    doAddFeats(selectedSubset, orderedCandFeats)
  }

  private def factorial(n:BigInt):BigInt = {
    require(n >= 0, "factorial of negatives is undefined")

    def doFactorial(n:BigInt,result:BigInt):BigInt = 
      if (n==0) result else doFactorial(n-1,n*result)

    doFactorial(n,1)
  }

  // TODO n = 50K is supported, 75K isn't.
  private def combinations(n: Int, r: Int): Int = {
    if (n < r) 0
    else if (n == r) 1
    else (factorial(n) / (factorial(r) * factorial(n-r))).toInt
  }
}

// /**
//  * :: Experimental ::
//  * Model fitted by [[ReliefFSelector]].
//  */
// @Experimental
// final class CfsSelectorModel private[ml] (selectedFeats: Array[Int]) {
//     extends Model[CfsSelectorModel] with ReliefFSelectorParams 
//     // with MLWritable 
//   {

//   /** @group setParam */
//   def setSelectionThreshold(value: Double): this.type = 
//     set(selectionThreshold, value)

//   /** @group setParam */
//   def setFeaturesCol(value: String): this.type = set(featuresCol, value)

//   /** @group setParam */
//   def setOutputCol(value: String): this.type = set(outputCol, value)

//   /** @group setParam */
//   def setLabelCol(value: String): this.type = set(labelCol, value)

//   override def transform(data: DataFrame): DataFrame = {

//     val selectedFeatures: Array[Int] = {

//         // Sorted features from most relevant to least (weight, index)
//         val sortedFeats: Array[(Double, Int)] = 
//           (featuresWeights.zipWithIndex).sorted(Ordering.by[(Double, Int), Double](_._1 * -1.0)).toArray

//         // Slice according threshold
//         (sortedFeats
//           .slice(0,(sortedFeats.size * $(selectionThreshold)).round.toInt)
//           .map(_._2))
//       }

//     //   if(useKnnSelection) {
        
//     //     val weights: Map[Int, Double] = (featuresWeights.indices zip featuresWeights).toMap

//     //     knnBestFeatures(weights, 0.5, -0.5)

//     //   } 

//     val slicer = (new VectorSlicer()
//       .setInputCol(featuresCol.name)
//       .setOutputCol(outputCol.name)
//       .setIndices(selectedFeatures))

//     // Return reduced Dataframe
//     // (slicer
//     //   .transform(data)
//     //   .selectExpr("selectedFeatures as features", "label"))
//     slicer.transform(data)
//   }


//   def saveResults(basePath: String): Unit = {
    
//     println("Adding weights to file:")
//     var file = new java.io.FileWriter(s"${basePath}_feats_weights.txt", true)
//     file.write(featuresWeights.head.toString)
//     featuresWeights.tail.foreach(weight => file.write("," + weight.toString))
//     file.write("\n")
//     file.close

//     println("saving positive feats:")
//     var weights: Map[Int, Double] = (featuresWeights.indices zip featuresWeights).toMap
//     var bestFeatures: Array[Int] = 
//       weights.filter{ case (k: Int, w: Double) => w > 0.0 }
//              .map{ case (k: Int, w: Double) => k }.toArray
//     file = new java.io.FileWriter(s"${basePath}_feats_positive.txt", true)
//     bestFeatures.foreach(feat => file.write(feat.toString + "\n"))
//     file.close
//     println("total: " + bestFeatures.size)

//     println("saving 10% best feats:")
//     val sortedFeats: Array[(Int, Double)] = 
//       (featuresWeights.indices zip featuresWeights).sorted(Ordering.by[(Int, Double), Double](_._2 * -1.0)).toArray
//     val bestFeats10Perc = 
//       sortedFeats.slice(0,(sortedFeats.size * 0.10).round.toInt).map(_._1)
//     file = new java.io.FileWriter(s"${basePath}_feats_10perc.txt", true)
//     bestFeats10Perc.foreach(feat => file.write(feat.toString + "\n"))
//     file.close
//     println("total: " + bestFeats10Perc.size)

//     println("saving 25% best feats:")
//     val bestFeats25Perc = 
//       sortedFeats.slice(0,(sortedFeats.size * 0.25).round.toInt).map(_._1)
//     file = new java.io.FileWriter(s"${basePath}_feats_25perc.txt", true)
//     bestFeats25Perc.foreach(feat => file.write(feat.toString + "\n"))
//     file.close
//     println("total: " + bestFeats25Perc.size)
    
//     println("saving 50% best feats:")
//     val bestFeats50Perc = 
//       sortedFeats.slice(0,(sortedFeats.size * 0.50).round.toInt).map(_._1)
//     file = new java.io.FileWriter(s"${basePath}_feats_50perc.txt", true)
//     bestFeats50Perc.foreach(feat => file.write(feat.toString + "\n"))
//     file.close
//     println("total: " + bestFeats50Perc.size)
    
//     println("saving 75% best feats:")
//     val bestFeats75Perc = 
//       sortedFeats.slice(0,(sortedFeats.size * 0.75).round.toInt).map(_._1)
//     file = new java.io.FileWriter(s"${basePath}_feats_75perc.txt", true)
//     bestFeats75Perc.foreach(feat => file.write(feat.toString + "\n"))
//     file.close
//     println("total: " + bestFeats75Perc.size)

//     println("saving hits contribution:")
//     file = new java.io.FileWriter(s"${basePath}_hits_contrib.txt", true)
//     file.write(totalHitsContributions.toString)
//     file.close

//     // println("saving knn best feats:")
//     // weights = (featuresWeights.indices zip featuresWeights).toMap
//     // bestFeatures = knnBestFeatures(weights, 0.5, -0.5)
//     // file = new java.io.FileWriter(s"${basePath}_feats_knn.txt", true)
//     // bestFeatures.foreach(feat => file.write(feat.toString + "\n"))
//     // file.close
//     // println("total: " + bestFeatures.size)

//   }

//   override def copy(extra: org.apache.spark.ml.param.ParamMap): org.apache.spark.ml.feature.CfsSelectorModel = ???

//   def transformSchema(schema: org.apache.spark.sql.types.StructType): org.apache.spark.sql.types.StructType = ???

// }