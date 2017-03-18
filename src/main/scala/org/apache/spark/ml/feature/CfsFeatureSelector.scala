package org.apache.spark.ml.feature

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import scala.collection.mutable.BitSet

class CfsFeatureSelector {

  // Searches a subset of features given the data
  // Returns a BitSet containing the selected features
  // df DataFrame must be discretized
  // If partitionSize == nFeats in df, then no partitioning is perfomed
  def fit(
    df: DataFrame,
    resultsFileBasePath: String,
    addLocalFeats: Boolean, 
    maxFails: Int,
    partitionSize: Int,
    restrictPartitionSizeIncrease: Boolean): BitSet = {

    val nFeats = df.take(1)(0)(0).asInstanceOf[Vector].size
    val nInstances: Long = df.count
    // By convention it is considered that class is stored after the last
    // feature in the correlations matrix
    val iClass = nFeats

    require(partitionSize == nFeats || partitionSize <= nFeats/2,
      "Partition size must be less than or equal to half size of the total number of feats or it must be equal to it")

    // TODO CorrelationsMatrix could be cleaned after a some features are
    // discarded!
    // remainingFeats are not necessarly in order (140,141,142, etc)
    def findSubset(
      remainingFeats: BitSet, 
      entropies: IndexedSeq[Double],
      correlations: CorrelationsMatrix,
      forceSinglePartition: Boolean = false): BitSet = {

      // The first time this function is called correlations matrix is empty
      val isFirstTime = correlations.isEmpty
      // If remaningFeats.size < partitionSize then nPartitions == 1
      val nPartitions: Int = {
        if (forceSinglePartition) 1
        else if (remainingFeats.size % partitionSize == 0)
            remainingFeats.size / partitionSize 
        else remainingFeats.size / partitionSize + 1
      }
      // Sequential splits of partitionSize from features Ex.: If
      // remainingFeats.size = 5, 2 size splits are: [[0,1],[2,3],[4]]
      val partitions: Seq[BitSet] = {
        (0 until nPartitions - 1).map{ i => 
          Range(i*partitionSize, (i+1) * partitionSize)
        } :+ 
        Range((nPartitions-1) * partitionSize, remainingFeats.size)
      // Partitions indexes must be mapped to the remainingFeats
      }.map{ p =>  BitSet(p.map(remainingFeats.toSeq):_*) }

      // DEBUG
      println("partitions:")
      println(partitions.mkString("\n"))

      // Feats pairs whose correlations have not been evaluated, 
      // Its important to keep (i,j), where i < j
      val remainingFeatsPairs: Seq[(Int,Int)] = 
        partitions.flatMap{ partition => 
          // The first time partitions include the class 
          val partitionAndClass = {
            if(isFirstTime) partition + iClass
            else partition
          }
          partitionAndClass.toSeq.combinations(2)
            .filter{ pair =>
              !correlations.keys.contains(pair(0), pair(1)) }
            .map{ pair => (pair(0), pair(1)) }
        }

      // DEBUG
      println("remainingFeats:")
      println(remainingFeats.mkString(" "))
      println("remainingFeatsPairs:")
      println(remainingFeatsPairs.mkString(" "))

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
      println(correlations.toString)

      val subsetEvaluator = new CfsSubsetEvaluator(correlations, iClass)

      // Search subsets on each partition, merge results.
      val newRemainingFeats: BitSet = 
        findAndMergeSubsetsInPartitions(partitions, subsetEvaluator, maxFails)

      //DEBUG
      println("NUMBER OF EVALUATIONS=" + subsetEvaluator.numOfEvaluations)
      println("BEST SUBSET = " + newRemainingFeats.toString)
  
      // End when the features where not partitioned
      if(nPartitions == 1){

        // Add locally predictive feats if requested
        if (!addLocalFeats)
          newRemainingFeats
        else 
          addLocallyPredictiveFeats(newRemainingFeats, correlations, iClass)

      // If after partitioned search the resulting feature subset has not
      // decreased in size use restrictPartitionSizeIncrease to decide whether 
      // returning subset as it is or trying to evaluate it with a single 
      // partition search.
      } else if(newRemainingFeats.size == remainingFeats.size) {

        if(restrictPartitionSizeIncrease) { 

          // Add locally predictive feats if requested
          if (!addLocalFeats)
            newRemainingFeats
          else 
            addLocallyPredictiveFeats(newRemainingFeats, correlations, iClass)
          
        } else {
          findSubset(newRemainingFeats,
            correlator.entropies, correlations, forceSinglePartition=true)
        }

      } else {
        findSubset(newRemainingFeats, correlator.entropies, correlations)
      }
    }

    findSubset(
      remainingFeats = BitSet(Range(0, nFeats):_*), 
      entropies = IndexedSeq(),
      correlations = new CorrelationsMatrix)

  }

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