// GA tests

import org.apache.spark.ml.feature
import scala.collection.immutable.BitSet

// Mutator
println("Testing mutation")
val fs = feature.FeaturesSubset(10)
println("fs.genotype = " + fs.genotype.toString)
var mutatedFs: feature.Individual[BitSet] = _
var i = 0
do {
  mutatedFs = (new feature.FeaturesSubsetMutator).mutate(fs)
  i += 1
} while (mutatedFs.genotype == fs.genotype)
println("mutatedFs.genotype = " + mutatedFs.genotype.toString)
println(s"number of test before mutation = $i")

// Matchmaker

val fsA = feature.FeaturesSubset(10)
fsA.genotype
val fsB = feature.FeaturesSubset(10)
fsB.genotype
i = 0
var crossoveredFs: (feature.Individual[BitSet],feature.Individual[BitSet]) = _
do {
  crossoveredFs = 
    (new feature.UniformFeaturesSubsetMatchmaker).crossover(fsA, fsB)
  i += 1
} while (fsA.genotype == fsA2.genotype && fsB.genotype == fsB2.genotype)
crossoveredFs._1.genotype
crossoveredFs._2.genotype
println(s"number of test before crossover = $i")


// Discretizer
import org.apache.spark.ml.feature._
import org.apache.spark.SparkContext
import org.apache.spark.SparkException
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.attribute._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.functions.udf

import org.apache.spark.ml.feature.QuantileDiscretizer

  // Parsing error handling implicit class methods
  // Using this class a call to rdd.tryMap can be done
  implicit class RDDParsing(lines: RDD[String]) {
    
    // Tries to map the received function to the rdd elements
    // accumulating all the exceptions in the received accumulator
    // A HashSet is used because since the adding is being done
    // inside a transformation (a not inside an action), there is no
    // guarantee that it won't get duplicated.
    def tryMap(
      f: (String) => Row,
      accum: CollectionAccumulator[(String, Throwable)]): RDD[Row] = {

      lines.flatMap(e => {
        val fe = Try{f(e)}
        val trial = fe match {
          case Failure(t) =>
            accum.add((e, t))
            fe
          case Success(t) => 
            fe
          // case t: Try[U] => t
        }
        // A Success is converted to Some and a Failure to None since flatMap
        // flattes collections, Nones will be considered empty collections and
        // only obtained values will be returned.
        trial.toOption
      })
    }
  }


  def readDFFromArff(
    fLocation: String, 
    discretize: Boolean, 
    nBinsPerColumn: Array[Int] = Array())
    : (DataFrame, Buffer[(String, Throwable)]) = {

    val sc = SparkContext.getOrCreate
    val spark = SparkSession.builder().getOrCreate()

    val lines = sc.textFile(fLocation)

    // Select lines starting with @attribute
    val attrLines: Array[Array[String]] = 
      lines.filter(_.startsWith("@attribute")).map(_.split(" ")).collect
      // TODO Receive the number of attributes?? (check next line)
      // lines.take(700).filter(_.startsWith("@attribute")).map(_.split(" "))

    // Only data lines, no header or comments
    val dataLines: RDD[String] = 
      (lines
        .filter{ l => !(l.startsWith("@")) && !(l.startsWith("%"))  }
      )

    // Parse attribute array from ARFF file header
    val attrs: Array[Attribute] = 
      (attrLines
        .map { line =>
          // Example lines (splitted): 
          // [@attribute, PredSA_r2, {0,1,2,3,4}]
          // [@attribute, AA_freq_central_A, real]
          // "class" attribute is renamed to "label"
          val attrName = if (line(1) == "class") "label" else line(1)
          val attrValuesOrType = line(2)

          // Nominal attribute
          if (attrValuesOrType.contains("{")) {
            // Strip { } and split to array
            val attrValues: Array[String] = 
              (attrValuesOrType
                .slice(1, attrValuesOrType.size - 1)
                .split(",")
              )

            NominalAttribute.defaultAttr.withName(attrName).withValues(attrValues)
          
          // Numeric attribute
          } else {
            NumericAttribute.defaultAttr.withName(attrName)
          }
        }
      )

    // Read and parse data to double features
    val broadAttrs = sc.broadcast(attrs)

    // Replaces strings to indexes and returns a Row of doubles
    def parseLineToRow(line: String): Row = {
      
      val splitted = line.split(",")
      require(splitted.size == broadAttrs.value.size, 
        s"Found a line with different number of attrs than declared: \n$line")

      // Replace line values with doubles
      val doublesLine: Array[Double] = 
        splitted.zip(broadAttrs.value).map { 
          case (value, attr) =>
            if(attr.isNominal) {
              attr.asInstanceOf[NominalAttribute].values
                match {
                  case Some(values:Array[String]) => 
                    values.indexOf(value).toDouble
                  case None => 
                    throw new SparkException(
                      s"Found nominal attribute with no values: $attr")
                }
            } else {
              value.toDouble
            }
        }
      // Create Row
      Row.fromSeq(doublesLine)
    }

    // Define an accumulator for parsing errors
    val parseExceptions = sc.collectionAccumulator[(String, Throwable)]

    // Parse using tryMap, to prevent a parsing error to stop the process,
    // parseExceptions cannot be immediatiely printed because, they inside
    // a transformation (lazy)
    val doublesRDD = dataLines.tryMap(parseLineToRow, parseExceptions)

    // Create schema from attributes
    val fields: Seq[StructField] = attrs.map{ attr => {
      val attrName = attr.name.get
      StructField(attrName, DoubleType, false, attr.toMetadata)
    }}
    val dfSchema = StructType(fields)

    // Create DataFrame
    val doublesDF = spark.createDataFrame(doublesRDD, dfSchema)  
    val (df, inputCols) =
      if(discretize) {      

        def discretizeNominal(df: DataFrame, idx: Int): DataFrame = {
          // Try to discretize everything except the label
          if(idx < attrs.size - 1){
            if(attrs(idx).isNumeric){
              val name = attrs(idx).name.get
              println("DISCRETIZING:" + name)
              val discretizer = (new QuantileDiscretizer()
                .setNumBuckets(nBinsPerColumn(idx))
                .setInputCol(name)
                .setOutputCol(name + "-discretized")
              )
              discretizeNominal(discretizer.fit(df).transform(df), idx + 1)
            } else discretizeNominal(df, idx + 1)
          } else df
        }

        val discretizedDF = discretizeNominal(doublesDF, 0)

        val inputCols = 
          attrs.map{ attr =>
            if(attr.isNumeric)
              attr.name.get + "-discretized"
            else
              attr.name.get
          }.filter(_ != "label")

        (discretizedDF, inputCols)
      } else {
        val inputCols = 
          attrs.map(_.name.get).filter(_ != "label")
        
        (doublesDF, inputCols)
      }

    // Merge features except class
    // This merge conserves the metadata
    val assembler = (new VectorAssembler()
      .setInputCols(inputCols)
      .setOutputCol("features")
    )

    // Curiosly enough, the collectionAccumulator returns a java.io.List
    val exceptions: Buffer[(String, Throwable)] =
      parseExceptions.value.asScala

    // Return the dataframe and a parsing exceptions list
    (assembler.transform(df).select("features", "label"), exceptions)

  }


val conf = new SparkConf().setAppName("spark-cfs").setMaster("local[*]")
val sc = new SparkContext(conf)
val spark = SparkSession.builder().getOrCreate()

val (df, excepts) = readDFFromArff("/home/raul/Datasets/Large/ECBDL14/head1000.arff", discretize=true, nBinsPerColumn=Array(9,11,32,4,4,4,4,3,4,4,4,4,4,4,4,4,3,4,4,4,4,12,12,22,21,22,11,27,17,21,28,13,25,6,18,8,19,26,15,6,6,6,6,5,6,6,6,6,6,6,6,6,5,6,6,6,6,6,6,6,6,5,6,6,6,6,6,6,6,6,5,6,6,6,6,6,6,6,6,5,6,6,6,6,6,6,6,6,5,6,6,6,6,40,46,60,27,69,31,31,33,60,29,32,50,28,44,34,33,20,37,11,6,12,20,8,8,13,31,10,8,8,14,8,5,7,20,13,7,6,11,3,3,3,3,3,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,27,24,14,29,20,27,26,20,23,21,36,7,18,11,27,17,25,7,14,24,4,4,3,4,4,3,3,5,4,3,4,5,2,5,6,2,5,5,6,3,4,4,3,4,3,3,4,5,3,4,3,4,2,3,6,3,5,5,4,5,4,4,4,5,3,4,4,5,4,4,2,5,2,5,6,3,5,3,5,6,5,5,5,6,4,5,4,6,6,5,3,6,2,4,8,4,5,2,4,6,8,7,9,8,6,7,8,8,7,9,8,7,7,7,7,6,7,4,4,8,4,6,4,5,3,3,5,5,4,4,2,4,2,2,6,4,5,2,3,5,4,5,4,4,3,3,3,4,3,4,2,4,3,3,5,4,3,2,3,5,5,3,4,4,3,5,4,3,2,4,4,3,2,2,3,3,3,3,3,4,4,2,3,4,3,2,2,4,2,2,3,2,3,3,4,2,3,2,5,3,4,2,2,2,3,2,2,5,2,2,3,2,3,2,2,2,2,2,2,2,4,3,3,4,3,4,4,4,2,3,2,3,2,2,2,3,3,2,2,3,2,2,3,3,3,2,2,4,2,3,3,2,2,3,2,2,4,2,2,4,2,4,3,4,2,3,3,4,2,3,4,2,2,2,3,3,4,3,3,4,7,5,7,8,6,8,7,7,6,8,7,6,6,7,6,6,4,5,4,7,4,5,4,5,3,3,4,5,3,4,4,5,3,4,5,5,5,3,2,5,2,4,4,5,3,4,4,4,4,4,2,4,2,4,7,5,5,3,3,6,4,3,4,5,2,3,4,4,4,3,3,3,2,3,5,4,4,2,4,4,4,3,5,5,5,4,4,4,3,3,4,4,3,4,5,4,5,4,2,4,2,3,2,4,3,2,2,2,3,2,2,3,3,2,4,2,3,2,3,2,2,3,2,4,3,2,2,2,3,2,2,2,2,2,3,2,3,3,3,2,2,3,2,3,4,2,2,2,3,2,2,2,2,3,3,2,3,3,3,2,2,2,2,3,4,2,3,2,3,2,3,2,3,3,4,3,3,3,3,2,2,2,2,2,3,2,2,2,3,2,2,2,2,2,3,3,2,2,3,2))





































// Print errors in case they happened
if (!excepts.isEmpty) {
  println("Exceptions found during parsing:")
  excepts.foreach(println)  
}





def extractFeat(idx: Int) = udf{ (x:Vector) => x(idx) }
val ag = AttributeGroup.fromStructField(df.schema("features"))
val attrs: Array[Attribute] = ag.attributes.get

val nFeats = 631
def disassembleFeats(df: DataFrame, idx: Int): DataFrame = {
  if(idx >= 0)
    disassembleFeats(
      df.withColumn(attrs(idx).name.get, extractFeat(idx)(df("features"))), 
      idx - 1
    )
  else
    df
}

val disassembledDF = disassembleFeats(df, nFeats - 1)






val df = spark.read.parquet("/home/raul/Datasets/Large/ECBDL14/head1000_train.parquet")

df.cache
// val discreteDF = Main.discretizeDF(df)


// Converts "features" column with 
// type mllib.linalg.DenseVector to ml.linalg.DenseVector
// Problem: Metadata is lost!
val convertVector = udf{ (features: org.apache.spark.mllib.linalg.DenseVector) => features.asML }
val upDF = df.withColum("features", convertVector(df("features")))





val ag = AttributeGroup.fromStructField(discreteDF.schema("features"))
val attrs: Array[Attribute] = ag.attributes.get
println(attrs.map{_.inde.get}.mkString(","))


def discretizeNominal(df: DataFrame, idx: Int): DataFrame = {
  if(idx >= 0){
    if(attrs(idx).isNominal){
      val name = attrs(idx).name.get
      val discretizer = (new QuantileDiscretizer()
        .setNumBuckets(10)
        .setInputCol(name)
        .setOutputCol("name" + "-discretized")
      )
      discretizer.fit(df).transform(df)
    } else discretizeNominal(df, idx - 1)
  } else df
}

val discretizedDF = discretizeNominal(disassembledDF)

val di_df = discretizer.fit(df).transform(df)

val ag = AttributeGroup.fromStructField(dis2DF.schema("features-discretized"))
val attrs: Array[Attribute] = ag.attributes.get
println(attrs.map{_.attrType.name}.mkString(","))













val nBinsPerColumn=Array(9,11,32,4,4,4,4,3,4,4,4,4,4,4,4,4,3,4,4,4,4,12,12,22,21,22,11,27,17,21,28,13,25,6,18,8,19,26,15,6,6,6,6,5,6,6,6,6,6,6,6,6,5,6,6,6,6,6,6,6,6,5,6,6,6,6,6,6,6,6,5,6,6,6,6,6,6,6,6,5,6,6,6,6,6,6,6,6,5,6,6,6,6,40,46,60,27,69,31,31,33,60,29,32,50,28,44,34,33,20,37,11,6,12,20,8,8,13,31,10,8,8,14,8,5,7,20,13,7,6,11,3,3,3,3,3,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,27,24,14,29,20,27,26,20,23,21,36,7,18,11,27,17,25,7,14,24,4,4,3,4,4,3,3,5,4,3,4,5,2,5,6,2,5,5,6,3,4,4,3,4,3,3,4,5,3,4,3,4,2,3,6,3,5,5,4,5,4,4,4,5,3,4,4,5,4,4,2,5,2,5,6,3,5,3,5,6,5,5,5,6,4,5,4,6,6,5,3,6,2,4,8,4,5,2,4,6,8,7,9,8,6,7,8,8,7,9,8,7,7,7,7,6,7,4,4,8,4,6,4,5,3,3,5,5,4,4,2,4,2,2,6,4,5,2,3,5,4,5,4,4,3,3,3,4,3,4,2,4,3,3,5,4,3,2,3,5,5,3,4,4,3,5,4,3,2,4,4,3,2,2,3,3,3,3,3,4,4,2,3,4,3,2,2,4,2,2,3,2,3,3,4,2,3,2,5,3,4,2,2,2,3,2,2,5,2,2,3,2,3,2,2,2,2,2,2,2,4,3,3,4,3,4,4,4,2,3,2,3,2,2,2,3,3,2,2,3,2,2,3,3,3,2,2,4,2,3,3,2,2,3,2,2,4,2,2,4,2,4,3,4,2,3,3,4,2,3,4,2,2,2,3,3,4,3,3,4,7,5,7,8,6,8,7,7,6,8,7,6,6,7,6,6,4,5,4,7,4,5,4,5,3,3,4,5,3,4,4,5,3,4,5,5,5,3,2,5,2,4,4,5,3,4,4,4,4,4,2,4,2,4,7,5,5,3,3,6,4,3,4,5,2,3,4,4,4,3,3,3,2,3,5,4,4,2,4,4,4,3,5,5,5,4,4,4,3,3,4,4,3,4,5,4,5,4,2,4,2,3,2,4,3,2,2,2,3,2,2,3,3,2,4,2,3,2,3,2,2,3,2,4,3,2,2,2,3,2,2,2,2,2,3,2,3,3,3,2,2,3,2,3,4,2,2,2,3,2,2,2,2,3,3,2,3,3,3,2,2,2,2,3,4,2,3,2,3,2,3,2,3,3,4,3,3,3,3,2,2,2,2,2,3,2,2,2,3,2,2,2,2,2,3,3,2,2,3,2))

val attrsTypes = Array(true, true, true, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true)

val attrsNames = Array("separation","propensity","length","PredSS_r1_-4","PredSS_r1_-3","PredSS_r1_-2","PredSS_r1_-1","PredSS_r1","PredSS_r1_1","PredSS_r1_2","PredSS_r1_3","PredSS_r1_4","PredSS_r2_-4","PredSS_r2_-3","PredSS_r2_-2","PredSS_r2_-1","PredSS_r2","PredSS_r2_1","PredSS_r2_2","PredSS_r2_3","PredSS_r2_4","PredSS_freq_cent","PredSS_freq_cent","PredSS_freq_cent","PredCN_freq_cent","PredCN_freq_cent","PredCN_freq_cent","PredCN_freq_cent","PredCN_freq_cent","PredRCH_freq_cen","PredRCH_freq_cen","PredRCH_freq_cen","PredRCH_freq_cen","PredRCH_freq_cen","PredSA_freq_cent","PredSA_freq_cent","PredSA_freq_cent","PredSA_freq_cent","PredSA_freq_cent","PredRCH_r1_-4","PredRCH_r1_-3","PredRCH_r1_-2","PredRCH_r1_-1","PredRCH_r1","PredRCH_r1_1","PredRCH_r1_2","PredRCH_r1_3","PredRCH_r1_4","PredRCH_r2_-4","PredRCH_r2_-3","PredRCH_r2_-2","PredRCH_r2_-1","PredRCH_r2","PredRCH_r2_1","PredRCH_r2_2","PredRCH_r2_3","PredRCH_r2_4","PredCN_r1_-4","PredCN_r1_-3","PredCN_r1_-2","PredCN_r1_-1","PredCN_r1","PredCN_r1_1","PredCN_r1_2","PredCN_r1_3","PredCN_r1_4","PredCN_r2_-4","PredCN_r2_-3","PredCN_r2_-2","PredCN_r2_-1","PredCN_r2","PredCN_r2_1","PredCN_r2_2","PredCN_r2_3","PredCN_r2_4","PredSA_r1_-4","PredSA_r1_-3","PredSA_r1_-2","PredSA_r1_-1","PredSA_r1","PredSA_r1_1","PredSA_r1_2","PredSA_r1_3","PredSA_r1_4","PredSA_r2_-4","PredSA_r2_-3","PredSA_r2_-2","PredSA_r2_-1","PredSA_r2","PredSA_r2_1","PredSA_r2_2","PredSA_r2_3","PredSA_r2_4","PredSS_freq_glob","PredSS_freq_glob","PredSS_freq_glob","PredCN_freq_glob","PredCN_freq_glob","PredCN_freq_glob","PredCN_freq_glob","PredCN_freq_glob","PredRCH_freq_glo","PredRCH_freq_glo","PredRCH_freq_glo","PredRCH_freq_glo","PredRCH_freq_glo","PredSA_freq_glob","PredSA_freq_glob","PredSA_freq_glob","PredSA_freq_glob","PredSA_freq_glob","AA_freq_central_","AA_freq_central_","AA_freq_central_","AA_freq_central_","AA_freq_central_","AA_freq_central_","AA_freq_central_","AA_freq_central_","AA_freq_central_","AA_freq_central_","AA_freq_central_","AA_freq_central_","AA_freq_central_","AA_freq_central_","AA_freq_central_","AA_freq_central_","AA_freq_central_","AA_freq_central_","AA_freq_central_","AA_freq_central_","PredSS_central_-","PredSS_central_-","PredSS_central","PredSS_central_1","PredSS_central_2","PredCN_central_-","PredCN_central_-","PredCN_central","PredCN_central_1","PredCN_central_2","PredRCH_central_","PredRCH_central_","PredRCH_central","PredRCH_central_","PredRCH_central_","PredSA_central_-","PredSA_central_-","PredSA_central","PredSA_central_1","PredSA_central_2","AA_freq_global_A","AA_freq_global_R","AA_freq_global_N","AA_freq_global_D","AA_freq_global_C","AA_freq_global_Q","AA_freq_global_E","AA_freq_global_G","AA_freq_global_H","AA_freq_global_I","AA_freq_global_L","AA_freq_global_K","AA_freq_global_M","AA_freq_global_F","AA_freq_global_P","AA_freq_global_S","AA_freq_global_T","AA_freq_global_W","AA_freq_global_Y","AA_freq_global_V","PSSM_r1_-4_A","PSSM_r1_-4_R","PSSM_r1_-4_N","PSSM_r1_-4_D","PSSM_r1_-4_C","PSSM_r1_-4_Q","PSSM_r1_-4_E","PSSM_r1_-4_G","PSSM_r1_-4_H","PSSM_r1_-4_I","PSSM_r1_-4_L","PSSM_r1_-4_K","PSSM_r1_-4_M","PSSM_r1_-4_F","PSSM_r1_-4_P","PSSM_r1_-4_S","PSSM_r1_-4_T","PSSM_r1_-4_W","PSSM_r1_-4_Y","PSSM_r1_-4_V","PSSM_r1_-3_A","PSSM_r1_-3_R","PSSM_r1_-3_N","PSSM_r1_-3_D","PSSM_r1_-3_C","PSSM_r1_-3_Q","PSSM_r1_-3_E","PSSM_r1_-3_G","PSSM_r1_-3_H","PSSM_r1_-3_I","PSSM_r1_-3_L","PSSM_r1_-3_K","PSSM_r1_-3_M","PSSM_r1_-3_F","PSSM_r1_-3_P","PSSM_r1_-3_S","PSSM_r1_-3_T","PSSM_r1_-3_W","PSSM_r1_-3_Y","PSSM_r1_-3_V","PSSM_r1_-2_A","PSSM_r1_-2_R","PSSM_r1_-2_N","PSSM_r1_-2_D","PSSM_r1_-2_C","PSSM_r1_-2_Q","PSSM_r1_-2_E","PSSM_r1_-2_G","PSSM_r1_-2_H","PSSM_r1_-2_I","PSSM_r1_-2_L","PSSM_r1_-2_K","PSSM_r1_-2_M","PSSM_r1_-2_F","PSSM_r1_-2_P","PSSM_r1_-2_S","PSSM_r1_-2_T","PSSM_r1_-2_W","PSSM_r1_-2_Y","PSSM_r1_-2_V","PSSM_r1_-1_A","PSSM_r1_-1_R","PSSM_r1_-1_N","PSSM_r1_-1_D","PSSM_r1_-1_C","PSSM_r1_-1_Q","PSSM_r1_-1_E","PSSM_r1_-1_G","PSSM_r1_-1_H","PSSM_r1_-1_I","PSSM_r1_-1_L","PSSM_r1_-1_K","PSSM_r1_-1_M","PSSM_r1_-1_F","PSSM_r1_-1_P","PSSM_r1_-1_S","PSSM_r1_-1_T","PSSM_r1_-1_W","PSSM_r1_-1_Y","PSSM_r1_-1_V","PSSM_r1_0_A","PSSM_r1_0_R","PSSM_r1_0_N","PSSM_r1_0_D","PSSM_r1_0_C","PSSM_r1_0_Q","PSSM_r1_0_E","PSSM_r1_0_G","PSSM_r1_0_H","PSSM_r1_0_I","PSSM_r1_0_L","PSSM_r1_0_K","PSSM_r1_0_M","PSSM_r1_0_F","PSSM_r1_0_P","PSSM_r1_0_S","PSSM_r1_0_T","PSSM_r1_0_W","PSSM_r1_0_Y","PSSM_r1_0_V","PSSM_r1_1_A","PSSM_r1_1_R","PSSM_r1_1_N","PSSM_r1_1_D","PSSM_r1_1_C","PSSM_r1_1_Q","PSSM_r1_1_E","PSSM_r1_1_G","PSSM_r1_1_H","PSSM_r1_1_I","PSSM_r1_1_L","PSSM_r1_1_K","PSSM_r1_1_M","PSSM_r1_1_F","PSSM_r1_1_P","PSSM_r1_1_S","PSSM_r1_1_T","PSSM_r1_1_W","PSSM_r1_1_Y","PSSM_r1_1_V","PSSM_r1_2_A","PSSM_r1_2_R","PSSM_r1_2_N","PSSM_r1_2_D","PSSM_r1_2_C","PSSM_r1_2_Q","PSSM_r1_2_E","PSSM_r1_2_G","PSSM_r1_2_H","PSSM_r1_2_I","PSSM_r1_2_L","PSSM_r1_2_K","PSSM_r1_2_M","PSSM_r1_2_F","PSSM_r1_2_P","PSSM_r1_2_S","PSSM_r1_2_T","PSSM_r1_2_W","PSSM_r1_2_Y","PSSM_r1_2_V","PSSM_r1_3_A","PSSM_r1_3_R","PSSM_r1_3_N","PSSM_r1_3_D")

def discretizeDF(df: DataFrame, lstart: Int, lend: Int, nBinsPerColumn: Int)
  : DataFrame = {

  def discretizeNominal(idx: Int): DataFrame = {
    // Try to discretize everything except the label
    // if(idx < nColumns - 1){
    if(idx < lend){
      // if(attrs(idx).isNumeric){
      if(attrsTypes){
        // val name = attrs(idx).name.get
        val name = attrsNames(idx)
        val discretizer = (new QuantileDiscretizer()
          .setNumBuckets(nBinsPerColumn(idx))
          .setInputCol(name)
          .setOutputCol(name + "-discretized")
        )
        // Test if caching if needed here!
        discretizeNominal(
          discretizer.fit(df).transform(df), idx + 1)
      } else discretizeNominal(df, idx + 1)
    } else df
  }

  discretizeNominal(lstart)
}



val limits = Array(0,161,231,301,371,441,511,581,630)
val df = spark.read.parquet(args(0))

for( i <- 0 to limits.size - 2) {

  val lstart = limits(i)
  val lend = limits(i + 1)
  
  val reducedDF = df.select(attrsNames(lstart), attrsNames.slice(lstart + 1,lend):_*).persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY)

  val discretizedDF = discretizeDF(reducedDF, lstart, lend, nBinsPerColumn)
  discretizedDF.write.format("parquet").save(args(0).split('.').head + "discretized" + lstart.toString + "-" + (lend-1).toString + ".parquet")
  

}






import scala.util.control.Breaks._
def trueTypeCounter(start:Int): Int = {
  var c = 0
  for( i <- Range(start, attrsTypes.size)){
    if (attrsTypes(i)) { c += 1 } 
    if (c == 70) { return i }
  }
  return attrsTypes.size - 1
}







