package org.apache.spark.ml.feature

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Since
import org.apache.spark.ml._
import org.apache.spark.ml.attribute.{AttributeGroup, _}
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructType}

import scala.collection.immutable.BitSet

/**
 * Params for [[CFSSelector]] and [[CFSSelectorModel]].
 */
private[feature] trait CFSSelectorParams extends Params
  with HasFeaturesCol with HasOutputCol with HasLabelCol {
  /**
   * Whether to add or not locally predictive feats as described by: Hall, M.
   * A. (2000). Correlation-based Feature Selection for Discrete and Numeric
   * Class Machine Learning.
   * The default value for locallyPredictive is true.
   *
   * @group param
   */
  final val locallyPredictive = new BooleanParam(this, "locallyPredictive",
    "Whether to add or not locally predictive feats as described by: Hall, M. A. (2000). Correlation-based Feature Selection for Discrete and Numeric Class Machine Learning.")
  setDefault(locallyPredictive -> true)

  /** @group getParam */
  def getLocallyPredictive: Boolean = $(locallyPredictive)

  /**
   * The initial size of the partitions. This is used in high dimensional
   * spaces where processing all features in a single partition is untractable.
   * Having initPartitionSize equal to zero or to the total number of
   * features implies processing all features in a single partition as the
   * original version of CFS does.
   * The default value for initPartitionSize is 0.
   *
   * @group param
   */
  final val initPartitionSize = new IntParam(this, "initPartitionSize",
    "The initial size of the partitions. This is used in high dimensional spaces where processing all features in a single partition is untractable. Having initPartitionSize equal to zero or to the total number of features implies processing all features in a single partition as the original version of CFS does. If this is not the case, then initPartitionSize must be less than or equal to half size of the total number of features in the dataset.",
    ParamValidators.gtEq(0))
  setDefault(initPartitionSize -> 0)

  /** @group getParam */
  def getInitialPartitionSize: Int = $(initPartitionSize)

  /**
   * The number of consecutive non-improving nodes to allow before terminating
   * the search (best first). 
   * The default value for searchTermination is 5.
   *
   * @group param
   */
  final val searchTermination = new IntParam(this, "searchTermination",
    "The number of consecutive non-improving nodes to allow before terminating the search (best first)",
    ParamValidators.gtEq(1))
  setDefault(searchTermination -> 5)

  /** @group getParam */
  def getSearchTermination: Int = $(searchTermination)


}


final class CFSSelector(override val uid: String)
  extends Estimator[CFSSelectorModel] 
  with CFSSelectorParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("cfsSelector"))

  /** @group setParam */
  def setInitPartitionSize(value: Int): this.type = set(initPartitionSize, value)

  /** @group setParam */
  def setSearchTermination(value: Int): this.type = set(searchTermination, value)

  /** @group setParam */
  def setLocallyPredictive(value: Boolean): this.type = set(locallyPredictive, value)

  /** @group setParam */
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setLabelCol(value: String): this.type = set(labelCol, value)

  override def fit(dataset: Dataset[_]): CFSSelectorModel = {
    transformSchema(dataset.schema, logging = true)

    val selectedFeats: BitSet = doFit(dataset)

    copyValues(new CFSSelectorModel(uid, selectedFeats.toArray).setParent(this)) 
  }

  override def transformSchema(schema: StructType): StructType = {

    val nFeats = 
      AttributeGroup.fromStructField(schema($(featuresCol))).size 

    // TODO DEBUG
    require(
      $(initPartitionSize) == nFeats || 
      $(initPartitionSize) == 0 || 
      $(initPartitionSize) <= nFeats/2,
      "If partition size is not zero or to the total number of feats, then it must be less than or equal to half size of that total number of feats.")

    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    SchemaUtils.checkNumericType(schema, $(labelCol))
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }

  override def copy(extra: ParamMap): CFSSelector = defaultCopy(extra)

  // Searches a subset of features given the data
  // dataset DataFrame must be discretized
  // If $(initPartitionSize) == nFeats in dataset, then no partitioning is perfomed
  private def doFit(dataset: Dataset[_]): BitSet = {

    // TODO DEBUG
    val nFeats = AttributeGroup.fromStructField(dataset.schema($(featuresCol))).size 
    val nInstances: Long = dataset.count
    // By convention it is considered that class is stored after the last
    // feature in the correlations matrix
    val iClass = nFeats

     // Calculate corrs with class for sorting
    val remainingFeatsPairs = Range(0, nFeats).zip(Vector.fill(nFeats)(iClass))
    val ctm = ContingencyTablesMatrix(
      dsToDF(dataset), 
      remainingFeatsPairs,
      nFeats + 1, // nFeats must include the class
      precalcEntropies=true)
    val correlator = new SymmetricUncertaintyCorrelator(
      ctm, nInstances, iClass)
    val correlations = new CorrelationsMatrix(nFeats)
    // Update correlations matrix (with corrs with class)
    correlations.update(remainingFeatsPairs, correlator)
    // Sort remainingFeats by their corr with class (from max to min)
    val remainingFeats = Range(0, nFeats).sorted(
      Ordering.by[Int, Double]( iFeat => correlations(iFeat, iClass) * -1.0))

    // DEBUG
    println(s"CORRS WITH CLASS = ${correlations.toStringCorrsWithClass}")

    def findSubset(
      currentPartitionSize: Int,
      remainingFeats: Seq[Int], 
      correlations: CorrelationsMatrix,
      correlator: SymmetricUncertaintyCorrelator,
      initialFeatsPairsCount: Int=0): BitSet = {

      val (partitions: Seq[Seq[Int]], remainingFeatsPairs: Seq[(Int,Int)]) = 
        getPartitionsAndRemainingFeatsPairs(
          currentPartitionSize,
          remainingFeats,
          correlations)

      // DEBUG
      // println("partitions:")
      // println(partitions.mkString("\n"))
      // println("remainingFeats:")
      // println(remainingFeats.mkString(" "))
      // println("remainingFeatsPairs:")
      // println(remainingFeatsPairs.mkString(" "))
      println(s"PARTITION SIZE = $currentPartitionSize")
      println(s"ABOUT TO PROCESS ${remainingFeatsPairs.size} FEATS PAIRS...")
      // DEBUG TEST
      val pairsCount = countRemainingFeatsPairs(
        currentPartitionSize,
        remainingFeats,
        correlations)
      require(pairsCount == remainingFeatsPairs.size, 
        s"ERROR: pairsCount != remainingFeatsPairs.size ($pairsCount != ${remainingFeatsPairs.size})")
      // END-DEBUG

      // The hard-work
      val newCtm = ContingencyTablesMatrix(dsToDF(dataset), remainingFeatsPairs)
      
      correlator.ctm = newCtm
      // Update correlations matrix
      correlations.update(remainingFeatsPairs, correlator)
      // Create a new evaluator (just to skip passing it as param)
      val subsetEvaluator = new CfsSubsetEvaluator(correlations, iClass)
      // Search subsets on each partition, merge (and sort) results.
      val newRemainingFeats: Seq[Int] = 
        findAndMergeSubsetsInPartitions(
          partitions, correlations, subsetEvaluator, $(searchTermination))

      // Clean unneeded correlations (keep corrs with class)
      correlations.clean(newRemainingFeats)

      //DEBUG
      println(s"NUMBER OF EVALUATIONS = ${subsetEvaluator.numOfEvaluations}")
      println(s"BEST SUBSET =  ${newRemainingFeats.toString} SIZE = ${newRemainingFeats.size}")

      // End when the features where not partitioned
      // Observe the adjustedPartitionSize warranties that the single partition
      // case will be always reached 
      if(partitions.size != 1){

        // Try to increase the partition size so the number of pair
        // correlations calculated is the maximum allowed by the
        // $(initPartitionSize)
        val checkedInitialFeatsPairsCount = 
          if (initialFeatsPairsCount == 0) remainingFeatsPairs.size 
          else initialFeatsPairsCount
        val adjustedPartitionSize = 
          getAdjustedPartitionSize(
            initialFeatsPairsCount = checkedInitialFeatsPairsCount,
            currentPartitionSize,
            newRemainingFeats,
            correlations)

        findSubset(
          adjustedPartitionSize, 
          newRemainingFeats, 
          correlations,
          correlator, 
          checkedInitialFeatsPairsCount)

      } else {

        val newRemainingFeatsSet = BitSet(newRemainingFeats:_*)

        // Add locally predictive feats if requested
        if (!$(locallyPredictive))
          newRemainingFeatsSet
        else 
          addLocallyPredictiveFeats(newRemainingFeatsSet, correlations, iClass)

      }
    }

    findSubset(
      $(initPartitionSize), remainingFeats, correlations, correlator)
  }


  private def getPartitionsAndRemainingFeatsPairs(
    partitionSize: Int, 
    remainingFeats: Seq[Int],
    correlations: CorrelationsMatrix): (Seq[Seq[Int]], Seq[(Int,Int)]) = {

    // If remaningFeats.size < partitionSize then nPartitions == 1
    val nPartitions: Int = {
      if (remainingFeats.size % partitionSize == 0)
        remainingFeats.size / partitionSize 
      else remainingFeats.size / partitionSize + 1
    }

    // Sequential splits of partitionSize from features Ex.: If
    // remainingFeats.size = 5, 2 size splits are: [[0,1],[2,3],[4]]
    val partitions: Seq[Seq[Int]] = {
      (0 until nPartitions - 1).map{ i => 
        Range(i * partitionSize, (i+1) * partitionSize).map(remainingFeats)
      } :+ 
      Range((nPartitions-1) * partitionSize, remainingFeats.size)
        .map(remainingFeats)
    }

    // Feats pairs whose correlations have not been evaluated, 
    // Its important to keep (i,j), where i < j
    val remainingFeatsPairs: Seq[(Int,Int)] = {
      partitions
        .flatMap( _.combinations(2) )
        .map{ pair => 
          if (pair(0) < pair(1)) (pair(0),pair(1)) else (pair(1),pair(0)) 
        }
        .diff(correlations.keys)
    }
    

    (partitions, remainingFeatsPairs)
  }

  // A formula based version didn't work because is not enough to substract
  // corrsWithClass from correlations to obtain the useful already calculated
  // correlations, this is because there could be correlations that were
  // calculated and right now are not useful because their feats fall in
  // diferent partitions but could be useful in future.
  private def countRemainingFeatsPairs(
    partitionSize: Int, 
    remainingFeats: Seq[Int],
    correlations: CorrelationsMatrix): Int = {

    val (_, remainingFeatsPairs: Seq[(Int,Int)]) = 
      getPartitionsAndRemainingFeatsPairs(
        partitionSize,
        remainingFeats,
        correlations)

    remainingFeatsPairs.size
  }

  private def getAdjustedPartitionSize(
    initialFeatsPairsCount: Int,
    currentPartitionSize: Int,
    remainingFeats: Seq[Int],
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

  private def findAndMergeSubsetsInPartitions(
    partitions: Seq[Seq[Int]], 
    correlations: CorrelationsMatrix,
    evaluator: CfsSubsetEvaluator,
    maxFails: Int): Seq[Int] = {

    val mergedFeats: Seq[Int] = 
      partitions.flatMap{ partition => 
        val optimizer = 
          new BestFirstSearcher(
            initialState = new FeaturesSubset(BitSet(), BitSet(partition:_*)),
            evaluator,
            maxFails)
        val result: EvaluatedState[BitSet] = optimizer.search

        //DEBUG
        if(partitions.size == 1){
          println(s"BEST MERIT= ${result.merit}")
        }
        
        result.state.data
      }

    // Due to the BitSet conversion, sorting is lost
    mergedFeats.sorted(
      Ordering.by[Int, Double]( 
        iFeat => correlations(iFeat, correlations.nFeats) * -1.0))
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

  private def dsToDF(dataset: Dataset[_]): DataFrame = {
    dataset.select(col($(featuresCol)), col($(labelCol)).cast(DoubleType))
  }
}

object CFSSelector extends DefaultParamsReadable[CFSSelector] {

  override def load(path: String): CFSSelector = super.load(path)
}

final class CFSSelectorModel private[ml] (
  override val uid: String,
  val selectedFeats: Array[Int])
  extends Model[CFSSelectorModel] with CFSSelectorParams with MLWritable {

  import CFSSelectorModel._

  /** @group setParam */
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val slicer = { new VectorSlicer()
      .setInputCol($(featuresCol))
      .setOutputCol($(outputCol))
      .setIndices(selectedFeats)
    }

    slicer.transform(dataset)
  }

  /**
   * There is no need to implement this method because all the work is done
   * by the VectorSlicer
   */
  override def transformSchema(schema: StructType): StructType = ???

  override def copy(extra: ParamMap): CFSSelectorModel = {
    val copied = new CFSSelectorModel(uid, selectedFeats)
    copyValues(copied, extra).setParent(parent)
  }

  override def write: MLWriter = new CFSSelectorModelWriter(this)
}

object CFSSelectorModel extends MLReadable[CFSSelectorModel] {

  private[CFSSelectorModel]
  class CFSSelectorModelWriter(instance: CFSSelectorModel) extends MLWriter {

    private case class Data(selectedFeats: Seq[Int])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.selectedFeats.toSeq)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class CFSSelectorModelReader extends MLReader[CFSSelectorModel] {

    private val className = classOf[CFSSelectorModel].getName

    override def load(path: String): CFSSelectorModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath).select("selectedFeats").head()
      val selectedFeats = data.getAs[Seq[Int]](0).toArray
      val model = new CFSSelectorModel(metadata.uid, selectedFeats)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  override def read: MLReader[CFSSelectorModel] = new CFSSelectorModelReader

  override def load(path: String): CFSSelectorModel = super.load(path)
}