package org.apache.spark.ml.feature

import scala.math.round

// Creates a correlations matrix for all features and class from dataset
// nFeats parameter should include the class
class CorrelationsMatrix(correlator: Correlator, val nFeats: Int) {

  // Correlate each feature f with f + 1 until last
  private var linearMatrix: IndexedSeq[Double] = 
    (0 until nFeats).flatMap { iFeatA =>
      (iFeatA + 1 until nFeats)
        .map{ iFeatB => correlator.correlate(iFeatA, iFeatB) }
    }

  def apply(i:Int, j:Int): Double = {
    if(i == j) {
      correlator.sameFeatureValue
    // Only pairs (i,j) where i < j are stored
    } else if(i < j){
      // Calculate position on linear storage
      val position = i*nFeats - 0.5*i*(i + 1) - i + j - 1
      assert(position.isWhole, "No whole position in correlation matrix")
      linearMatrix(position.toInt) 
    // Invert i and j if i > j
    } else {
      // Calculate position on linear storage
      val position = j*nFeats - 0.5*j*(j + 1) - j + i - 1
      assert(position.isWhole, "No whole position in correlation matrix")
      linearMatrix(position.toInt)
    }
  }
}


// ------------ DISTRIBUTED MATRIX FAILED VERSION --------------- //

// Spark does not allow to start a task from another task, so it is not
// possible to create an RDD from the results of task from another. In
// consecuence, a solution could be to create a local matrix with a part of the
// correlations, then parallelize, join and so on, iteratively create a bigger
// matrix.

// // Correlate each feature f with f + 1 until last
// distrCorrs = sc
//   .parallelize(0 until nFeats)
//   .flatMap {
//     featA => 
//       (featA + 1 until nFeats)
//         .map{ featB => ((featA, featB), 
//                         correlator.correlate(featA,featB)) 
//         }
//   }
//   // Since parallelize acts lazily, there should be no problem
//   // with defining the Partitioner "after" the flatMap
//   .partitionBy(
//     new CorrelationsMatrixPartitioner(sc.defaultParallelism, nFeats))

// distrCorrs.cache()

// nPartitions must be >= 1
// class CorrelationsMatrixPartitioner(
//   nPartitions: Int,
//   nFeats: Int) extends Partitioner {

//   // Total number of elements in linear storage
//   private val nElements = (nFeats - 2)*nFeats + 
//                           ((2 - nFeats)*(nFeats - 1))/2 + 1

//   private val partitionSize = round(nElements/nPartitions).toInt
  
//   // For a given key returns the partitionId it corresponds to.
//   def getPartition(key: Any): Int = {

//     val (i,j) = key.asInstanceOf[(Int,Int)]
//     // Calculate position on linear storage
//     val position = (i - 1)*nFeats + j - (i - 1)*i/2 - i - 1

//     val partitionId = round(position/partitionSize - 0.5).toInt

//     // When the last partition has more elements than the partition size,
//     // the element has to be included on it
//     if(partitionId <= nPartitions - 1) partitionId else (nPartitions - 1)
//   }

//   def numPartitions: Int = nPartitions

// }

