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
  rdd: RDD[Array[Byte]], attrsSizes: IndexedSeq[Int])
  extends Correlator(sameFeatureValue=1.0) {

  var totalPairsEvaluated = 0
  
  // By convention it is considered that class is stored after the last
  // feature in the correlations matrix
  private val iClass = attrsSizes.size - 1
  // The smallest deviation allowed in double comparisons. (from WEKA)
  private val SMALL: Double = 1e-6

  // TODO Check is useful to cache entropies
  private var entropies = IndexedSeq.empty[Double]

  private var bEntropies: Broadcast[IndexedSeq[Double]] = _
  // Broadcasted sizes for ctables creation
  private val bCtSizes = rdd.context.broadcast(attrsSizes)

  // ContingencyTables Int key represents the iPartner
  private var ctables: RDD[(Int, ContingencyTable)] = _

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

  private def calculateCTables(iFixedFeat: Int, iPartners: Seq[Int]):
    RDD[(Int, ContingencyTable)] = {

    val bIPartners = rdd.context.broadcast(iPartners)
    val iFixedFeatSize = attrsSizes(iFixedFeat)

    rdd.mapPartitions{ partition =>
      val rows: Array[Array[Byte]] = partition.toArray
      bIPartners.value.map{ iPartner =>
        val m = 
          DenseMatrix.zeros[Double](bCtSizes.value(iPartner), iFixedFeatSize)
        rows.foreach{ row => m(row(iPartner),row(iFixedFeat)) += 1.0 }
        (iPartner, new ContingencyTable(m))
      }.toIterator
    }.reduceByKey(_ + _).sortByKey()
  }

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
  
  // Calculates entropies for all feats including the class,
  // This method only works if ctables with iFixedFeat was the class 
  // and iPartners contained all the features
  private def calculateEntropies: IndexedSeq[Double] = {
    // val featsEntropies: IndexedSeq[Double] = 
    //   ctables
    //     .map{ case (iPartner, ct) => (iPartner, ct.rowsEntropy) } 
    //     .collect.sortBy(_._1).map(_._2)
    val featsEntropies: IndexedSeq[Double] = 
      ctables.map(pair => pair._2.rowsEntropy).collect
    val classEntropy: Double = ctables.first._2.colsEntropy

    featsEntropies :+ classEntropy
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

    // Hard work!
    ctables = calculateCTables(iFixedFeat, iPartners)
    
    if(entropies.isEmpty) {
      entropies = calculateEntropies
      bEntropies = ctables.context.broadcast(entropies)
      // DEBUG
      // println("ENTROPIES = ")
      // println(entropies.mkString(","))
    }

    val iFixedFeatEntropy = entropies(iFixedFeat)
    
    ctables.map{ case (iPartner, ct) => 

      val infoGain = 
        bEntropies.value(iPartner) - ct.condEntropy(iFixedFeatEntropy)
      val denom = bEntropies.value(iPartner) + iFixedFeatEntropy
      
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
    }.map(_._2).collect
    // }.collect.sortBy(_._1).map(_._2)
  }

  private def approxEq(a: Double, b: Double): Boolean = {
    ((a == b) || ((a - b < SMALL) && (b - a < SMALL)))
  }
}