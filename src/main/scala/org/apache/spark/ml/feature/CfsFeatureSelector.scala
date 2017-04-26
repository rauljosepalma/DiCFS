package org.apache.spark.ml.feature

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Since
import org.apache.spark.ml._
import org.apache.spark.ml.attribute.{AttributeGroup, _}
import org.apache.spark.ml.linalg.{Vector, VectorUDT,Vectors}
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
    val informativeDS = filterNonInformativeFeats(dataset)

    transformSchema(informativeDS.schema, logging = true)

    val selectedFeats: BitSet = doFit(informativeDS)
      
    val informativeAttrs: Array[Attribute] = AttributeGroup.fromStructField(
      informativeDS.schema($(featuresCol))).attributes.get

    val selectedFeatsNames: Array[String] = 
      selectedFeats.map(informativeAttrs(_).name.get).toArray

    val originalAG: AttributeGroup  = 
      AttributeGroup.fromStructField(dataset.schema($(featuresCol)))

    // DEBUG
    // Show feats indexes based on original dataset
    println("SELECTED FEATS = " + 
      selectedFeatsNames.map(originalAG.indexOf).sorted.mkString(","))

    copyValues(new CFSSelectorModel(uid, selectedFeatsNames).setParent(this)) 
  }

  override def transformSchema(schema: StructType): StructType = {

    val nFeats = AttributeGroup.fromStructField(schema($(featuresCol))).size

    // TODO DEBUG
    require(
      $(initPartitionSize) == nFeats || 
      $(initPartitionSize) == 0 || 
      $(initPartitionSize) <= nFeats/2,
      "If partition size is not zero or equal to the total number of (informative) feats, then it must be less than or equal to half size of that total number of (informative) feats.")

    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    SchemaUtils.checkNumericType(schema, $(labelCol))
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }

  override def copy(extra: ParamMap): CFSSelector = defaultCopy(extra)

  // DEBUG
  private def filterNonInformativeFeats(dataset: Dataset[_]): Dataset[_] = {
    val ag = AttributeGroup.fromStructField(dataset.schema($(featuresCol)))
    val attrs: Array[Attribute] = ag.attributes.get
    
    attrs.foreach{ attr => 
      require(attr.isNominal, "All features must represent nominal attributes in metadata")
      require(attr.name != None, "All features must have a name in metadata")
    }

    // Non informative feats contain a single value
    val informativeFeatsIndexes: Array[Int] = { attrs
      .zipWithIndex
      .filter{ case (attr, _) =>
        attr.asInstanceOf[NominalAttribute].getNumValues.get > 1
      }
      .map{ case (_, index) => index }
    }

    val informativeAttrs: AttributeGroup = 
      new AttributeGroup(ag.name, informativeFeatsIndexes.map(attrs))

    val nNonInformativeFeats = attrs.size - informativeFeatsIndexes.size
    
    if(nNonInformativeFeats > 0) {

      print(s"$nNonInformativeFeats feature(s) with a single value will not be considered")

      val informativeFeatsUDF = udf{ (features: Vector) => 
        Vectors.dense(
          informativeFeatsIndexes.map{ case (i: Int) => features(i) }) 
      }

      { dataset
        // Filter feats
        .withColumn(
          $(featuresCol),
          informativeFeatsUDF(col($(featuresCol))))
        // Preserve metadata
        .withColumn(
          $(featuresCol),
          col($(featuresCol)).as("_", informativeAttrs.toMetadata))
      }
      
    } else {
      dataset
    }
  }

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
    if($(initPartitionSize) == 0) setInitPartitionSize(nFeats)

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
    
    // Sort remainingSubsets by their corr with class (from max to min)
    val remainingSubsets: Seq[EvaluatedFeaturesSubset] = 
      Range(0, nFeats).map{ 
        iFeat => new EvaluatedFeaturesSubset(
          new FeaturesSubset(BitSet(iFeat), BitSet(iFeat)), 
          correlations(iFeat, iClass))
      }.sortWith(_ > _)

    // DEBUG
    // println(s"CORRS WITH CLASS = ${correlations.toStringCorrsWithClass}")

    def findSubset(
      currentPartitionSize: Int,
      remainingSubsets: Seq[EvaluatedFeaturesSubset], 
      correlations: CorrelationsMatrix,
      correlator: SymmetricUncertaintyCorrelator,
      initialFeatsPairsCount: Int=0): BitSet = {

      val partitions: Seq[Seq[FeaturesSubset]] = 
        getStratifiedPartitions(currentPartitionSize, remainingSubsets)

      val remainingFeatsPairs: Seq[(Int,Int)] = 
        getRemainingFeatsPairs(partitions)

      // DEBUG
      // println("partitions:")
      // println(partitions.mkString("\n"))
      // println("remainingSubsets:")
      // println(remainingSubsets.mkString(" "))
      // println("remainingFeatsPairs:")
      // println(remainingFeatsPairs.mkString(" "))
      println(s"PARTITION SIZE = $currentPartitionSize")
      println(s"ABOUT TO PROCESS ${remainingFeatsPairs.size} FEATS PAIRS...")

      // The hard-work
      val newCtm = ContingencyTablesMatrix(dsToDF(dataset), remainingFeatsPairs)
      
      correlator.ctm = newCtm
      // Update correlations matrix
      correlations.update(remainingFeatsPairs, correlator)
      // Create a new evaluator (just to skip passing it as param)
      val subsetEvaluator = new CfsSubsetEvaluator(correlations, iClass)
      // Search subsets on each partition, sort results
      val newRemainingSubsets: Seq[EvaluatedFeaturesSubset] = 
        findSubsetsInPartitions(
          partitions, correlations, subsetEvaluator, $(searchTermination))

      // Clean unneeded correlations (keep corrs with class)
      correlations.clean(newRemainingSubsets)

      //DEBUG
      println(s"NUMBER OF EVALUATIONS = ${subsetEvaluator.numOfEvaluations}")
      println(s"FOUND ${newRemainingSubsets.size} SUBSETS = \n${newRemainingSubsets.mkString("\n")}")

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
            newRemainingSubsets)

        findSubset(
          adjustedPartitionSize, 
          newRemainingSubsets, 
          correlations,
          correlator, 
          checkedInitialFeatsPairsCount)

      } else {

        // Add locally predictive feats if requested
        if (!$(locallyPredictive))
          newRemainingSubsets(0).fSubset.data
        else 
          addLocallyPredictiveFeats(newRemainingSubsets(0).fSubset.data, 
            correlations, iClass)

      }
    }

    findSubset(
      $(initPartitionSize), remainingSubsets, correlations, correlator)
  }

  // TODO make a diagram of this method!!!
  // remainingSubsets must be sorted from greatest to least
  private def getStratifiedPartitions(
    partitionSize: Int, 
    remainingSubsets: Seq[EvaluatedFeaturesSubset])
    : Seq[Seq[FeaturesSubset]] = {
    
    // Put subsets in a sigle partition
    if(partitionSize >= remainingSubsets.size) {
      Seq(remainingSubsets.map(_.fSubset))
    } else {

      // Slices refer to the division in original sorted subsets
      // nPartitions represent also te size of each slice
      val nPartitions = remainingSubsets.size / partitionSize
      val nFirstSlices = partitionSize
      val firtSlices: Seq[Seq[FeaturesSubset]] = 
        (0 until nFirstSlices).map{ i =>
          Range(i * nPartitions, (i+1) * nPartitions)
            .map(remainingSubsets.map(_.fSubset))
        }
      val lastSlice: Seq[FeaturesSubset] = 
        Range(nFirstSlices * nPartitions, remainingSubsets.size)
          .map(remainingSubsets.map(_.fSubset))

      // Partitions are the definitive result
      val firstPartitions: Seq[Seq[FeaturesSubset]] = 
        (0 until nPartitions).map{ i =>
          firtSlices.map(_(i))
      }
      // add last slice elements to first partitions
      val completePartitions: Seq[Seq[FeaturesSubset]] = 
        firstPartitions.zipWithIndex.map{ 
          case (partition, idx) =>
            if (idx < lastSlice.size)
              partition :+ lastSlice(idx)
            else
              partition
        }

      completePartitions
    }

  }

  private def getAdjustedPartitionSize(
    initialFeatsPairsCount: Int,
    remainingSubsets: Seq[EvaluatedFeaturesSubset]): Int = {

    def factorial(n:BigInt):BigInt = {
      require(n >= 0, "factorial of negatives is undefined")
      def doFactorial(n:BigInt,result:BigInt):BigInt = 
        if (n==0) result else doFactorial(n-1,n*result)
      doFactorial(n,1)
    }
    // TODO n = 50K is supported, 75K isn't.
    def combinations(n: Int, r: Int): Int = {
      if (n < r) 0
      else if (n == r) 1
      else (factorial(n) / (factorial(r) * factorial(n-r))).toInt
    }

    val averageSubsetSize: Int = 
      remainingSubsets.map(_.fSubset.size).sum / remainingSubsets.size

    def doSearchPartitionSize(psize: Int): Int = {
      val featsPairsCount = 
        if(averageSubsetSize == 1)
          combinations(psize, 2) * remainingSubsets.size.toDouble / psize
        else
          scala.math.pow(averageSubsetSize, psize) * remainingSubsets.size.toDouble / psize

      if (featsPairsCount > initialFeatsPairsCount)
        psize - 1
      else if (psize == remainingSubsets.size) 
        psize
      else 
        doSearchPartitionSize(psize + 1)
    }

    doSearchPartitionSize(2)

  }

  // Feats pairs whose correlations have not been evaluated, 
  // Its important to keep (i,j), where i < j
  private def getRemainingFeatsPairs(
    partitions: Seq[Seq[FeaturesSubset]]): Seq[(Int, Int)] = {

    partitions.flatMap{ partition =>

      val mergedPartition: Seq[Int] = 
        partition.map(_.data).reduce(_ ++ _).toSeq

      val mergedPartitionFeatsPairs: Seq[(Int, Int)] = 
        mergedPartition.combinations(2).toSeq.map(p => (p(0),p(1)))

      val calculatedFeatsPairs: Seq[(Int,Int)] = partition.flatMap{
        _.data.toSeq.combinations(2).toSeq.map(p => (p(0),p(1)))
      }

      mergedPartitionFeatsPairs.diff(calculatedFeatsPairs)

    }
  }


  private def findSubsetsInPartitions(
    partitions: Seq[Seq[FeaturesSubset]], 
    correlations: CorrelationsMatrix,
    evaluator: CfsSubsetEvaluator,
    maxFails: Int): Seq[EvaluatedFeaturesSubset] = {

    partitions.map{ partition =>

      val mergedPartition: BitSet = partition.map(_.data).reduce(_ ++ _)

      val optimizer = 
        new BestFirstSearcher(
          initialState = new FeaturesSubset(BitSet(), mergedPartition),
          evaluator,
          maxFails)
      val result: EvaluatedState[BitSet] = optimizer.search

      //DEBUG
      if(partitions.size == 1){
        println(s"BEST MERIT= ${result.merit}")
      }

      new EvaluatedFeaturesSubset(
        new FeaturesSubset(result.state.data, mergedPartition), result.merit)

    }.sortWith(_ > _)

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
  val selectedFeatsNames: Array[String])
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
      .setNames(selectedFeatsNames)
    }

    slicer.transform(dataset)
  }

  /**
   * There is no need to implement this method because all the work is done
   * by the VectorSlicer
   */
  override def transformSchema(schema: StructType): StructType = ???

  override def copy(extra: ParamMap): CFSSelectorModel = {
    val copied = new CFSSelectorModel(uid, selectedFeatsNames)
    copyValues(copied, extra).setParent(parent)
  }

  override def write: MLWriter = new CFSSelectorModelWriter(this)
}

object CFSSelectorModel extends MLReadable[CFSSelectorModel] {

  private[CFSSelectorModel]
  class CFSSelectorModelWriter(instance: CFSSelectorModel) extends MLWriter {

    private case class Data(selectedFeatsNames: Seq[String])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.selectedFeatsNames.toSeq)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class CFSSelectorModelReader extends MLReader[CFSSelectorModel] {

    private val className = classOf[CFSSelectorModel].getName

    override def load(path: String): CFSSelectorModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath).select("selectedFeatsNames").head()
      val selectedFeatsNames = data.getAs[Seq[String]](0).toArray
      val model = new CFSSelectorModel(metadata.uid, selectedFeatsNames)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  override def read: MLReader[CFSSelectorModel] = new CFSSelectorModelReader

  override def load(path: String): CFSSelectorModel = super.load(path)
}