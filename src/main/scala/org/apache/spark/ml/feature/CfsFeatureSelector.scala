package org.apache.spark.ml.feature

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Since
import org.apache.spark.ml._
import org.apache.spark.ml.attribute._
import org.apache.spark.mllib.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructType, NumericType, DataType, StructField}
import org.apache.spark.rdd.RDD

import scala.util.{Try, Success, Failure}

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

  /**
   * Whether to use a vertical partitioning scheme to process the data.
   * Experiments show that for larger datasets setting this parameter to true
   * its a good idea. 
   * The default value for verticalPartitioning is true.
   *
   * @group param
   */
  final val verticalPartitioning = new BooleanParam(this, "verticalPartitioning",
    "Whether to use a vertical partitioning scheme to process the data. Experiments show that for larger datasets setting this parameter to true its a good idea.")
  setDefault(verticalPartitioning -> true)

  /** @group getParam */
  def getVerticalPartitioning: Boolean = $(verticalPartitioning)

  /**
   * If vertical partitioning is enabled, this indicates the number of
   * partitions to use after the data is transformed to a columnar format.
   * The default value is 0, which means that the number of partitions will be 
   * equal to the number of features in the dataset.
   *
   * @group param
   */
  final val nPartitions = new IntParam(this, "nPartitions",
    "If vertical partitioning is enabled, this indicates the number of partitions to use after the data is transformed to a columnar format.",
    ParamValidators.gtEq(0))
  setDefault(nPartitions -> 0)

  /** @group getParam */
  def getNPartitions: Int = $(nPartitions)
}


final class CFSSelector(override val uid: String)
  extends Estimator[CFSSelectorModel] 
  with CFSSelectorParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("cfsSelector"))

  /** @group setParam */
  def setSearchTermination(value: Int): this.type = set(searchTermination, value)

  /** @group setParam */
  def setLocallyPredictive(value: Boolean): this.type = set(locallyPredictive, value)
  
  /** @group setParam */
  def setVerticalPartitioning(value: Boolean): this.type = set(verticalPartitioning, value)

  /** @group setParam */
  def setNPartitions(value: Int): this.type = set(nPartitions, value)

  /** @group setParam */
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)
 
  /** @group setParam */
  def setLabelCol(value: String): this.type = set(labelCol, value)

  override def fit(data: DataFrame): CFSSelectorModel = {
    transformSchema(data.schema, logging = true)
    validateMetadata(data.schema)
    
    val (informativeDF, initialIndexesMap): (DataFrame, Array[Int]) = 
      filterNonInformativeFeats(data)

    val informativeDFSelectedFeats: FeaturesSubset = doFit(informativeDF)

    val selectedFeats: Array[Int] = 
      informativeDFSelectedFeats.map(initialIndexesMap).toArray

    // DEBUG
    // Show feats indexes based on original data
    println("SELECTED FEATS = " + selectedFeats.sorted.mkString(","))

    copyValues(new CFSSelectorModel(uid, selectedFeats).setParent(this)) 
  }

  override def transformSchema(schema: StructType): StructType = {

    // Imported methods from spark version 2.2.0
    def checkNumericType(
        schema: StructType,
        colName: String,
        msg: String = ""): Unit = {
      val actualDataType = schema(colName).dataType
      val message = if (msg != null && msg.trim.length > 0) " " + msg else ""
      require(actualDataType.isInstanceOf[NumericType], s"Column $colName must be of type " +
        s"NumericType but was actually of type $actualDataType.$message")
    }
    def appendColumn(
        schema: StructType,
        colName: String,
        dataType: DataType): StructType = {
      if (colName.isEmpty) return schema
      appendField(schema, StructField(colName, dataType, nullable=false))
    }
    def appendField(schema: StructType, col: StructField): StructType = {
      require(!schema.fieldNames.contains(col.name), s"Column ${col.name} already exists.")
      StructType(schema.fields :+ col)
    }
      
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    checkNumericType(schema, $(labelCol))
    appendColumn(schema, $(outputCol), new VectorUDT)
  }

  override def copy(extra: ParamMap): CFSSelector = defaultCopy(extra)

  // Returns the number of values of either a nominal or binary attribute
  // checking types of the attribute
  private def getNumValues(attr: Attribute): Int = {
    Try(attr.asInstanceOf[NominalAttribute]) match {
      case Success(nomAttr) =>
        nomAttr.getNumValues.getOrElse(throw new IllegalArgumentException("Attribute must have values defined in metadata"))
      case Failure(_) =>
        Try(attr.asInstanceOf[BinaryAttribute]).getOrElse(throw new IllegalArgumentException("Attribute type not supported. All features and label must represent nominal or binary attributes in metadata"))
        2
    }
  }

  private def validateMetadata(schema: StructType): Unit = {
    val ag = AttributeGroup.fromStructField(schema($(featuresCol)))
    val labelAttr = Attribute.fromStructField(schema($(labelCol)))

    val allAttrs: Array[Attribute] = 
      ag.attributes.getOrElse(throw new IllegalArgumentException("Dataset  must contain metadata for each feature and label")) :+ labelAttr

    allAttrs.foreach{ attr => 
      val numValues = getNumValues(attr)
      require(numValues > 0 && numValues <= 255, 
        "All features and label must have metadata values in range ]0,255]")
    }
  }

  private def filterNonInformativeFeats(df: DataFrame)
    : (DataFrame, Array[Int]) = {
    val ag = AttributeGroup.fromStructField(df.schema($(featuresCol)))
    val attrs: Array[Attribute] = ag.attributes.get
    
    // Non informative feats contain a single value
    val informativeFeatsIndexes: Array[Int] = { attrs
      .zipWithIndex
      .filter{ case (attr, _) => getNumValues(attr) > 1 }
      .map{ case (_, index) => index }
    }

    val informativeAttrs: AttributeGroup = 
      new AttributeGroup(ag.name, informativeFeatsIndexes.map(attrs))

    val nNonInformativeFeats = attrs.size - informativeFeatsIndexes.size
    
    if(nNonInformativeFeats > 0) {

      // DEBUG
      println(s"$nNonInformativeFeats feature(s) with a single value will not be considered")

      val informativeFeatsUDF = udf{ (features: Vector) => 
        Vectors.dense(
          informativeFeatsIndexes.map{ case (i: Int) => features(i) }) 
      }

      (df
        // Filter feats
        .withColumn(
          $(featuresCol),
          informativeFeatsUDF(col($(featuresCol))))
        // Preserve metadata
        .withColumn(
          $(featuresCol),
          col($(featuresCol)).as("_", informativeAttrs.toMetadata)),
      informativeFeatsIndexes)
      
    } else {
      (df, informativeFeatsIndexes) 
    }
  }

  private def doFit(df: DataFrame): FeaturesSubset = {

    val nFeats = 
      AttributeGroup.fromStructField(df.schema($(featuresCol)))
        .attributes.get.size
    // By convention it is considered that class is stored after the last
    // feature in the correlations matrix
    val iClass = nFeats

    val correlator = 
      if ($(verticalPartitioning))
        new SUCorrelatorVP(df, $(nPartitions))
      else
        new SUCorrelatorHP(df)

    val corrs = new CorrelationsMatrix(correlator)
    val evaluator = new CfsSubsetEvaluator(corrs, iClass)
    val optimizer = 
      new BestFirstSearcher(
        new FeaturesSubset(IndexedSeq(), Range(0, nFeats)),
        evaluator, $(searchTermination)
      )
    val result = optimizer.search
    val subset = result.state.asInstanceOf[FeaturesSubset]

    correlator.unpersistData
    
    // DEBUG
    println(s"BEST MERIT= ${result.merit}")
    println(s"TOTAL NUM OF EVALUATED PAIRS=${correlator.totalPairsEvaluated}") 
    println(s"TOTAL NUM OF PASSES=${evaluator.numOfPasses}")  
    
    // Add locally predictive feats if requested
    if (!$(locallyPredictive))
      subset
    else {
      val newSubset = addLocallyPredictiveFeats(subset, corrs, iClass)
      // DEBUG
      println(s"UPDATED NUM OF EVALUATED PAIRS=${correlator.totalPairsEvaluated}")  
      println(s"UPDATED NUM OF PASSES=${evaluator.numOfPasses}")  
      newSubset
    }
  }

  private def addLocallyPredictiveFeats(
    selectedSubset: FeaturesSubset, 
    corrs: CorrelationsMatrix, 
    iClass: Int) : FeaturesSubset = {

    // Descending order not selected feats according to their correlation with
    // the class
    val orderedCandFeats: Seq[Int] = 
      Range(0, iClass)
        .filter(!selectedSubset.contains(_))
        .sortWith{
          (a, b) => corrs(a, iClass) > corrs(b, iClass)}

    def doAddFeats(extendedSubset: FeaturesSubset, orderedCandFeats: Seq[Int])
      : FeaturesSubset = {

      if(orderedCandFeats.isEmpty){
        extendedSubset
      }else{
        // Check if next candidate feat is more correlated to the class than
        // to any of the selected feats
        val candFeat = orderedCandFeats.head
        val tempSubset = 
          extendedSubset
            .filter { f => (corrs(f,candFeat) > corrs(candFeat,iClass)) }
        // Add feat to the selected set
        if(tempSubset.isEmpty){
          // DEBUG
          println(s"ADDING LOCALLY PREDICTIVE FEAT: $candFeat")
          // iPartners must be asc sorted!
          corrs.precalcNonExistentCorrs(candFeat, orderedCandFeats.tail.sorted)
          doAddFeats(
            extendedSubset + candFeat, orderedCandFeats.tail)
        // Ignore feat
        }else{
          doAddFeats(extendedSubset, orderedCandFeats.tail)
        }
      }
    }

    doAddFeats(selectedSubset, orderedCandFeats)
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

  override def transform(data: DataFrame): DataFrame = {
    val slicer = { new VectorSlicer()
      .setInputCol($(featuresCol))
      .setOutputCol($(outputCol))
      .setIndices(selectedFeats)
    }

    slicer.transform(data)
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
      sqlContext.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class CFSSelectorModelReader extends MLReader[CFSSelectorModel] {

    private val className = classOf[CFSSelectorModel].getName

    override def load(path: String): CFSSelectorModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sqlContext.read.parquet(dataPath).select("selectedFeats").head()
      val selectedFeats = data.getAs[Seq[Int]](0).toArray
      val model = new CFSSelectorModel(metadata.uid, selectedFeats)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  override def read: MLReader[CFSSelectorModel] = new CFSSelectorModelReader

  override def load(path: String): CFSSelectorModel = super.load(path)
}