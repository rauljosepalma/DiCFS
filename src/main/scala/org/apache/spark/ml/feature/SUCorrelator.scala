package org.apache.spark.ml.feature

import org.apache.spark.Partitioner
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.ml.attribute._
import org.apache.spark.storage.StorageLevel

import breeze.linalg.DenseMatrix

import scala.util.Try

// sameFeature represents default value for two indentical features
abstract class Correlator extends Serializable {
  def correlate(iFixedFeat: Int, iPartners: Seq[Int]): Seq[Double]
}

// df must contain a fist column with features and a second with label
abstract class SUCorrelator(df: DataFrame) extends Correlator {

  // DEBUG
  var totalPairsEvaluated = 0

  // The amount of different values each feat has (including the class)
  protected val featsSizes: IndexedSeq[Int] = {
    val featuresAttrs: Array[Attribute] = 
      AttributeGroup.fromStructField(df.schema(0)).attributes.get
    val labelAttr: Attribute = Attribute.fromStructField(df.schema(1))

    featuresAttrs.map(getNumValues) :+ getNumValues(labelAttr)
  }
  // Broadcasted sizes for ctables creation
  // In case of autoSampling == true, the featsSizes could change, however
  // using the original sizes does not affect the final result
  protected val bFeatsSizes = df.rdd.context.broadcast(featsSizes)
  // By convention it is considered that class is stored at the end
  // In this case nFeats includes the class
  protected val nFeats = featsSizes.size
  protected val iClass = nFeats - 1
  // The smallest deviation allowed in double comparisons. (from WEKA)
  protected val SMALL: Double = 1e-6

  protected type DataFormat
  protected val data: RDD[DataFormat] = {
    // DEBUG
    val t0 = System.currentTimeMillis()

    val r = prepareData.persist(StorageLevel.MEMORY_ONLY)

    val t1 = System.currentTimeMillis()
    println("DATA PREPARATION TIME: " + (t1 - t0) + "ms")

    r
  }

  protected val (corrsWithClass, bEntropies): 
    (Seq[Double], Broadcast[IndexedSeq[Double]]) = {
    
    val ctables: RDD[(Int, ContingencyTable)] = 
      calculateCTables(iClass, Range(0, iClass))
    val entropies = calculateEntropies(ctables)
    val bEntropies = df.rdd.context.broadcast(entropies)
    val corrs = doCorrelate(iClass, Range(0, iClass), ctables, bEntropies)

    (corrs, bEntropies)
  }

  protected def prepareData: RDD[DataFormat]
  protected def calculateCTables(iFixedFeat: Int, iPartners: Seq[Int])
    : RDD[(Int, ContingencyTable)] 
  
  // Return correlations considering iPartners is in ascending order
  override def correlate(iFixedFeat: Int, iPartners: Seq[Int]): Seq[Double] = {

    require(!iPartners.isEmpty, 
      "Cannot create ContingencyTables with empty iPartners collection")

    // DEBUG
    // println(s"EVALUATING FEAT ${iFixedFeat} WITH ${iPartners.size} iPARTNERS:")
    // println(iPartners.mkString(","))
    
    totalPairsEvaluated += iPartners.size

    // The first time, corrs with class will be requested and they've 
    // already been calculated
    if(iFixedFeat == iClass && iPartners.size == iClass - 1){
      corrsWithClass
    } else {
      // Hard work!
      // ContingencyTables Int key represents the iPartner
      val ctables: RDD[(Int, ContingencyTable)] = 
        calculateCTables(iFixedFeat, iPartners)
  
      doCorrelate(iFixedFeat, iPartners, ctables, bEntropies)
    }
  }

  def unpersistData: Unit = data.unpersist()
 
  // Calculates entropies for all feats including the class,
  // This method only works if ctables with iFixedFeat was the class 
  // and iPartners contained all the features
  protected def calculateEntropies(ctables: RDD[(Int, ContingencyTable)])
    : IndexedSeq[Double] = {

    val featsEntropies: IndexedSeq[Double] = 
      ctables.sortByKey().map(pair => pair._2.rowsEntropy).collect
    val classEntropy: Double = ctables.first._2.colsEntropy

    featsEntropies :+ classEntropy
  }
  
  protected def doCorrelate(
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
      if(approxEq(correlation,0.0, threshold=SMALL))
        // if(iPartner == iClass || iFixedFeat == iClass) 0.0 else 1.0
        if(iPartner == iClass || iFixedFeat == iClass) (iPartner, 0.0) else (iPartner, 1.0)
      else
        // correlation
        (iPartner, correlation)
    // }.sortByKey().values.collect
    }.collectAsMap.toSeq.sortBy(_._1).map(_._2)
  }


  protected def approxEq(a: Double, b: Double, threshold: Double): Boolean = {
    ((a == b) || ((a - b < threshold) && (b - a < threshold)))
  }

  // If attribute is not nominal it is assumed to be binary
  protected def getNumValues(attr: Attribute): Int = 
    Try(attr.asInstanceOf[NominalAttribute]).map(_.getNumValues.get).getOrElse(2)
}

class SUCorrelatorHP(df: DataFrame) extends SUCorrelator(df) {

  type DataFormat = Array[Byte]

  override def prepareData: RDD[DataFormat] = {
    df.rdd.map{ row: Row =>
      val features = row(0).asInstanceOf[Vector].toArray.map(_.toByte)
      val label = row(1).asInstanceOf[Double].toByte

      features :+ label
    }
  }

  override protected def calculateCTables(iFixedFeat: Int, iPartners: Seq[Int])
    : RDD[(Int, ContingencyTable)] = {

    val bIPartners = data.context.broadcast(iPartners.toIndexedSeq)
    val iFixedFeatSize = featsSizes(iFixedFeat)

    // This approach proved much more efficient than converting the partition
    // to an array and then looping many times over it.
    data.mapPartitions{ partition =>
      val matrices: IndexedSeq[DenseMatrix[Double]] =
        bIPartners.value.map{ iPartner =>
          DenseMatrix.zeros[Double](bFeatsSizes.value(iPartner), 
            iFixedFeatSize)          
        }
      partition.foreach{ (row: Array[Byte]) => 
        bIPartners.value.zipWithIndex.foreach{ case (iPartner, idx) =>
          matrices(idx)(row(iPartner),row(iFixedFeat)) += 1.0
        }
      }
      matrices.zip(bIPartners.value).map{ case (matrix, iPartner) =>
        (iPartner, new ContingencyTable(matrix))
      }.toIterator
    }.reduceByKey(_ + _)
  }
}

class SUCorrelatorVP(df: DataFrame, nPartitions: Int)
  extends SUCorrelator(df) {

  type DataFormat = (Int, Array[Byte])

  override def prepareData: RDD[DataFormat] = {

    // TODO Check licensing
    // Ideas took from: 
    // https://github.com/sramirez/spark-infotheoretic-feature-selection
    // Apart from the transposed data optimization, not merging same 
    // feature-rows contributes to reduce much shuffling.
    // Data is equally distributed, so most partitions have approximate the
    // same number of rows.
    val initialNParts = df.rdd.getNumPartitions
    val nInstances = df.count
    val eqDistributedData: RDD[(Long, Row)] = 
      df.rdd.zipWithIndex().map(_.swap)
        .partitionBy(new ExactPartitioner(initialNParts, nInstances))

    // Its called columnarData because a full transpose is not performed. 
    // For every partition a transposed matrix with nRows = nFeats and with 
    // nCols = nInstances in partition is generated.
    val columnarData: RDD[(Int, Array[Byte])] = 
      eqDistributedData.mapPartitionsWithIndex{ (idxPart, partition) =>
        val rows: Array[Row] = partition.toArray.map(_._2)
        val transpData = Array.ofDim[Byte](nFeats, rows.length)
        rows.zipWithIndex.foreach{ case (row, iInstance) =>
          val features = row(0).asInstanceOf[Vector]
          val label = row(1).asInstanceOf[Double]
          (0 until nFeats - 1).foreach{ iFeat => 
            transpData(iFeat)(iInstance) =  features(iFeat).toByte
          }
          transpData(nFeats - 1)(iInstance) = label.toByte
        }
        // Add an special index to transpData used to represent the original
        // partition number and the feat each row belongs to.
        (0 until nFeats).map{ iFeat => 
          (iFeat * initialNParts + idxPart, transpData(iFeat))
        }.toIterator
      }

    // Sort to group all rows for the same feature closely in order to avoid
    // shuffling too much ContingencyTables later.
    if(nPartitions == 0)
      columnarData.sortByKey(numPartitions=nFeats)
    else
      columnarData.sortByKey(numPartitions=nPartitions)

  }

  override protected def calculateCTables(iFixedFeat: Int, iPartners: Seq[Int])
    : RDD[(Int, ContingencyTable)] = {

    // Create environment for code
    val bIPartners = df.rdd.context.broadcast(iPartners)
    val initialNParts = df.rdd.getNumPartitions

    // Select all rows in rdd that correspond to iFixedFeat, there will be 
    // one for each partition in the initial df
    val minIdx = iFixedFeat * initialNParts
    val fixedFeatRows = 
      data.filterByRange(minIdx, minIdx + initialNParts - 1).collect()
    val fixedCol = Array.ofDim[Array[Byte]](fixedFeatRows.length)
    fixedFeatRows.foreach{ case (idx, row) => 
      fixedCol(idx % initialNParts) = row }
    val bFixedCol=  data.context.broadcast(fixedCol)

    val ctables = 
      data
        .filter{ case (idx, _) => 
          bIPartners.value.contains(idx / initialNParts) }
        .mapPartitions{ partition =>
           var result = Map.empty[Int, DenseMatrix[Double]]
          partition.foreach{ case (index, arr) =>
              val feat = index / initialNParts
              val block = index % initialNParts
              val m = result.getOrElse(
                feat,
                // fixedFeat goes in cols
                DenseMatrix.zeros[Double](
                  bFeatsSizes.value(feat), 
                  bFeatsSizes.value(iFixedFeat))
              )
              (0 until arr.length).foreach{ i =>
                m(arr(i), bFixedCol.value(block)(i)) += 1.0
              }
              result += feat -> m
            }

          result.toIterator
        }
        .reduceByKey(_ + _) 
        .map(p=> (p._1, new ContingencyTable(p._2)))

    bFixedCol.unpersist()
    bIPartners.unpersist()

    ctables
  }

}
class ExactPartitioner(partitions: Int, elements: Long) extends Partitioner {
  override def numPartitions: Int = partitions
  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Long]
    return (k * partitions / elements).toInt
  }
}