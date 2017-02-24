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


  def readDFFromArFF(fLocation: String, discretize: Boolean): 
    (DataFrame, Buffer[(String, Throwable)]) = {

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
    val df = spark.createDataFrame(doublesRDD, dfSchema)  

    // Merge features except class
    // This merge conserves the metadata
    val inputCols = attrs.map{ attr =>
      attr.name match {
        case Some(name: String) => name
        case None => 
          throw new SparkException(
                  s"Found nominal attribute with no name: $attr")
      }
    }.filter(_ != "label")

    if(discretize) {      
      val discretizer = (new QuantileDiscretizer()
        .setNumBuckets(10)
        .setInputCol(name)
        .setOutputCol("name" + "-discretized")
      )
      discretizer.fit(df).transform(df)
    }

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

val (df, excepts) = Main.readArffToDF("/home/raul/Datasets/Large/ECBDL14/head1000.arff").
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


