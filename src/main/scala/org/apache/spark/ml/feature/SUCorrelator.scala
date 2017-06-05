package org.apache.spark.ml.feature

import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast

import breeze.linalg.DenseMatrix

// sameFeature represents default value for two indentical features
abstract class Correlator(val sameFeatureValue: Double) extends Serializable {
  def correlate(iFixedFeat: Int, iPartners: Seq[Int]): Seq[Double]
}

// attrs must contain Attribute objects for all feats including the class and
// the end
class SUCorrelator(
  origData: RDD[Array[Byte]], 
  attrsSizes: IndexedSeq[Int], 
  autoSampling: Boolean)
  extends Correlator(sameFeatureValue=1.0) {

  var totalPairsEvaluated = 0
  
  // By convention it is considered that class is stored after the last
  // feature in the correlations matrix
  val iClass = attrsSizes.size - 1
  // The smallest deviation allowed in double comparisons. (from WEKA)
  private val SMALL: Double = 1e-6

  // Broadcasted sizes for ctables creation
  // In case of autoSampling == true, the attrsSizes could change, however
  // using the original sizes does not affect the final result
  private val bCtSizes = origData.context.broadcast(attrsSizes)

  // private val nInstances = 30801723L
  private val batchSize = 0.05D 

  private val (data, corrsWithClass, bEntropies): 
    (RDD[Array[Byte]], Seq[Double], Broadcast[IndexedSeq[Double]]) = 
    
    if (autoSampling) {
      searchSample(Seq(), origData, Double.MaxValue, None)
    } else {
      val ctables: RDD[(Int, ContingencyTable)] = 
        calculateCTables(iClass, Range(0, iClass), origData)
      val entropies = calculateEntropies(ctables)
      val bEntropies = ctables.context.broadcast(entropies)
      val corrs = doCorrelate(iClass, Range(0, iClass), ctables, bEntropies)

      (origData, corrs, bEntropies)
    }
  
  // Return correlations in the same order of iPartners
  override def correlate(iFixedFeat: Int, iPartners: Seq[Int]): Seq[Double] = {

    require(!iPartners.isEmpty, 
      "Cannot create ContingencyTables with empty iPartners collection")

    // DEBUG
    println(s"EVALUATING FEAT ${iFixedFeat} WITH ${iPartners.size} iPARTNERS:")
    // println(iPartners.mkString(","))
    // val difFeats = t.flatMap(p => Seq(p._1,p._2)).distinct.size
    totalPairsEvaluated += iPartners.size

    // The first time, corrs with class will be requested and they've 
    // already been calculated
    if(iFixedFeat == iClass && iPartners.size == iClass - 1){
      corrsWithClass
    } else {
      // Hard work!
      // ContingencyTables Int key represents the iPartner
      val ctables: RDD[(Int, ContingencyTable)] = 
        calculateCTables(iFixedFeat, iPartners, data)
  
      doCorrelate(iFixedFeat, iPartners, ctables, bEntropies)
    }
  }

  private def searchSample(
    sampledBatches: Seq[RDD[Array[Byte]]],
    remainingData: RDD[Array[Byte]], 
    prevCorrsSum: Double,
    // prevSampleSize: Double,
    // An array is broadcasted to simplify serialization
    bPrevCTables: Option[Broadcast[Array[ContingencyTable]]])
    : (RDD[Array[Byte]], Seq[Double], Broadcast[IndexedSeq[Double]]) = {

    val splittedData = 
      remainingData.randomSplit(Array(batchSize, 1.0 - batchSize))
    val (newBatch, newRemainingData) = (splittedData(0), splittedData(1))
    // Hard work!
    val newCTables: RDD[(Int, ContingencyTable)] = 
      if(bPrevCTables.nonEmpty)
        calculateCTables(iClass, Range(0, iClass), newBatch)
          // Add newCTables to prevCTables
          .map{ case(iFeat,ct) => (iFeat, bPrevCTables.get.value(iFeat) + ct) }
      else
        calculateCTables(iClass, Range(0, iClass), newBatch)

    val entropies = calculateEntropies(newCTables)
    val bEntropies = remainingData.context.broadcast(entropies)
    val corrs = doCorrelate(iClass, Range(0, iClass), newCTables, bEntropies)
    // val newSampleSize = (1.0 - batchSize) * prevSampleSize + batchSize*nInstances

    bPrevCTables.foreach(_.unpersist())

    if (corrs.sum < prevCorrsSum) {
      val newCTablesArray: Array[ContingencyTable] = 
        newCTables.collectAsMap.toMap.toArray.sortBy(_._1).map(_._2)
      val bNewCTables = remainingData.context.broadcast(newCTablesArray)
      bEntropies.unpersist()
      
      searchSample(sampledBatches :+ newBatch, 
        newRemainingData, corrs.sum, Option(bNewCTables))
    } else {
      // newSampleSize
      // return the union all sample RDDs and corrs to prevent recalculation
      ((sampledBatches :+ newBatch).reduce(_ ++ _), corrs, bEntropies)
    } 
  }

  private def calculateCTables(
    iFixedFeat: Int, iPartners: Seq[Int], 
    data: RDD[Array[Byte]]): RDD[(Int, ContingencyTable)] = {

    val bIPartners = data.context.broadcast(iPartners)
    val iFixedFeatSize = attrsSizes(iFixedFeat)

    data.mapPartitions{ partition =>
      val rows: Array[Array[Byte]] = partition.toArray
      bIPartners.value.map{ iPartner =>
        val m = 
          DenseMatrix.zeros[Double](bCtSizes.value(iPartner), iFixedFeatSize)
        rows.foreach{ row => m(row(iPartner),row(iFixedFeat)) += 1.0 }
        (iPartner, new ContingencyTable(m))
      }.toIterator
    }.reduceByKey(_ + _)
  }
 
  // Calculates entropies for all feats including the class,
  // This method only works if ctables with iFixedFeat was the class 
  // and iPartners contained all the features
  private def calculateEntropies(ctables: RDD[(Int, ContingencyTable)])
    : IndexedSeq[Double] = {
    // val featsEntropies: IndexedSeq[Double] = 
    //   ctables
    //     .map{ case (iPartner, ct) => (iPartner, ct.rowsEntropy) } 
    //     .collect.sortBy(_._1).map(_._2)
    val featsEntropies: IndexedSeq[Double] = 
      ctables.sortByKey().map(pair => pair._2.rowsEntropy).collect
    val classEntropy: Double = ctables.first._2.colsEntropy

    featsEntropies :+ classEntropy
  }
  
  private def doCorrelate(
    iFixedFeat: Int, iPartners: Seq[Int], 
    ctables: RDD[(Int, ContingencyTable)], 
    bEntropies: Broadcast[IndexedSeq[Double]]): Seq[Double] = {

    ctables.map{ case (iPartner, ct) => 

      val fixedFeatEntropy = bEntropies.value(iFixedFeat)
      val partnerFeatEntropy = bEntropies.value(iPartner)
      val infoGain = partnerFeatEntropy - ct.condEntropy(fixedFeatEntropy)
      val denom = partnerFeatEntropy + fixedFeatEntropy
      
      // Two feats with a single value will have both zero entropies
      // and consecuently a denom == 0
      val correlation = if (denom == 0.0) 0.0 else (2.0 * infoGain / denom)

      
      // This condition was taken from WEKA source code     
      // If two classes have approximately zero correlation but neither of them
      // is the class, then their SymmetricUncertainty is considered to be 1.0
      if(approxEq(correlation,0.0))
        // if(iPartner == iClass || iFixedFeat == iClass) 0.0 else 1.0
        if(iPartner == iClass || iFixedFeat == iClass) (iPartner, 0.0) else (iPartner, 1.0)
      else
        // correlation
        (iPartner, correlation)
    }.sortByKey().values.collect
    // }.collect.sortBy(_._1).map(_._2)
  }

  private def approxEq(a: Double, b: Double): Boolean = {
    ((a == b) || ((a - b < SMALL) && (b - a < SMALL)))
  }

  // TODO DELETE?

  // private def calculateCTables(iFixedFeat: Int, iPartners: Seq[Int]):
  //   RDD[(Int, ContingencyTable)] = {
    
  //   val bIPartners = rdd.context.broadcast(iPartners)
  //   val iFixedFeatSize = attrsSizes(iFixedFeat)

  //   rdd.mapPartitions{ partition =>
  //     val localCTables: Seq[DenseMatrix[Double]] = 
  //       bIPartners.value.map{ iPartner => 
  //         DenseMatrix.zeros[Double](bCtSizes.value(iPartner), iFixedFeatSize)
  //       }
  //     partition.foreach{ row: Array[Byte] =>
  //       bIPartners.value.zipWithIndex.foreach{ 
  //         case (iPartner:Int, idx: Int) => ;
  //           localCTables(idx)(row(iPartner))(row(iFixedFeat)) += 1.0          
  //       }
        
  //       // bIPartners.value.zip(localCTables).foreach{ 
  //       //   case (iPartner:Int, localCTable:DenseMatrix[Double]) => ;
  //       //     localCTable(row(iPartner))(row(iFixedFeat)) += 1.0
  //       // }
  //     }

  //     bIPartners.value.zip(localCTables)
  //       .map{ case (iParent, matrix) => 
  //         (iParent, new ContingencyTable(matrix)) }.toIterator

  //     // localCTables.zip().map(new ContingencyTable(_)).toIterator
  //   }.reduceByKey(_ + _)
  // }

    // private def calculateCTables(pairs: Seq[(Int,Int)]) = {
  //   val bPairs = rdd.context.broadcast(pairs)
  //   ctables = rdd.mapPartitions{ partition =>
  //     val rows: Array[Array[Byte]] = partition.toArray
  //     bPairs.value.map{ case (i,j) =>
  //       val m = DenseMatrix.zeros[Double](bCtSizes.value(i), bCtSizes.value(j))
  //       rows.foreach{ row => m(row(i),row(j)) += 1.0 }
  //       ((i,j), new ContingencyTable(m))
  //     }.toIterator
  //   }.reduceByKey(_ + _)
  // }

}