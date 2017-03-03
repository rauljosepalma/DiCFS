package org.apache.spark.ml.feature

import org.apache.spark.SparkContext
import org.apache.spark.SparkException
import org.apache.spark.SparkConf
import org.apache.spark.Accumulable
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.attribute._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.util.CollectionAccumulator

// import org.apache.spark.mllib.feature.MDLPDiscretizer

import scala.collection.mutable
import scala.collection.mutable.Buffer
import scala.collection.immutable.IndexedSeq
import scala.collection.immutable.BitSet
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}



object Main {

  
  // Data preparation methods

  /*
   
   The steps are basically:

   Parse Attribute array from file
   Read and parse data to double features
   Merge feats in one column.
   Save to parquet.

    This object should be added as a Datasource in th future.
  */


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

  // This is used in case a long process is executed, the ssh pipe
  // can fail so is better to store output in the driver.
  def outputInfoLine(info: String) = {

    // try {
    val f = new java.io.FileWriter("/root/debug.txt", true)
    f.write(info + "\n")
    f.close
    // } catch {case x:Throwable => println(x.getMessage)}    
    
  }

  // Receives an ARFF file as a RDD of string and 
  // returns  DataFrame with two cols: features and label,
  // it also returns a HashSet with possible exceptions captured
  // during file parsing
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
    doublesDF.cache

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
              // Test if caching if needed here!
              discretizeNominal(
                discretizer.fit(df).transform(df).cache, idx + 1)
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


  // Reads an SVM file to a DataFrame (and correctly sets its schema)
  // Caution: some SVM datasets have too many feats, so initiliatizing 
  // its metadata is not feasible
  def readSVMToDF(fLocation:String, nFeats: Int, labels: Array[String]):
    DataFrame = {

    val spark = SparkSession.builder().getOrCreate()

    val featsArray: Array[Attribute] = ((0 until nFeats)
      .map { a => 
        val attr = (NumericAttribute.defaultAttr
          .withName(a.toString)
          .withIndex(a)
        )
        attr
      }.toArray
    )

    val featsAttrGroup = new AttributeGroup("features", featsArray)

    val labelAttr = 
      NominalAttribute.defaultAttr.withName("label").withValues(labels)

    val df = spark.read.format("libsvm").load(fLocation)
    
    // Add metadata to a DataFrame
    df.select(
      df("features").as("features", featsAttrGroup.toMetadata),
      df("label").as("label", labelAttr.toMetadata))
  }

  // Creates am AttributeGroup with NumericAttributes for the features column
  // and a NominalAttribute for the label column
  // def numFeatNomLabelSchGenerator(
  //   numOfFeats: Int,
  //   labels: Array[String]): StructType = {

  //   val featsArray: Array[Attribute] = ((0 until numOfFeats)
  //     .map { a => 
  //       val attr = (NumericAttribute.defaultAttr
  //         .withName(a.toString)
  //         .withIndex(a)
  //       )
  //     }
  //   )

  //   val featsAttrGroup = new AttributeGroup("features", featsArray)
  //   val featsField = 
  //     StructField("features", DoubleType, false, featsAttrGroup.toMetadata)

  //   val labelAttr = new NominalAttribute("label").withValues(labels)
  //   val labelField = 
  //     StructField("label", DoubleType, false, labelAttr.toMetadata)

  //   new StructType(Seq(featsField, labelField))

  // }


  // Parse libsvm to csv
  // val lines = sc.textFile("/home/raul/Desktop/Datasets/Large/EPSILON/epsilon_normalized.libsvm")
  // val splitted = lines.map(_.split(" "))
  // val regex = "[0-9]+:".r
  // val noidx = splitted.map(_.map(regex.replaceAllIn(_, "")))
  // val classLast = noidx.map(a => a.tail :+ a.head)
  // val outLines = classLast.map(_.mkString(","))
  // outLines.coalesce(1).saveAsTextFile("/home/raul/Desktop/Datasets/Large/EPSILON/EPSILON_train.csv")


  // End of data preparation methods

  // args:
  // args(0): file location
  // args(1): k (num of neighbors)
  // args(2): m (sample size)
  // Optional
  // args(3): num of feats.
  // args(4): class label
  // args(5): class label
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("spark-cfs")
    val sc = SparkContext.getOrCreate
    val spark = SparkSession.builder().getOrCreate()
        // Reduce verbosity
    // sc.setLogLevel("WARN")
    
    // val df = args(0).split('.').last match {
    //   case "arff" => 
    //     val (df1, excepts) = readDFFromArff(args(0), discretize=false, nBinsPerColumn=Array(9,11,32,4,4,4,4,3,4,4,4,4,4,4,4,4,3,4,4,4,4,12,12,22,21,22,11,27,17,21,28,13,25,6,18,8,19,26,15,6,6,6,6,5,6,6,6,6,6,6,6,6,5,6,6,6,6,6,6,6,6,5,6,6,6,6,6,6,6,6,5,6,6,6,6,6,6,6,6,5,6,6,6,6,6,6,6,6,5,6,6,6,6,40,46,60,27,69,31,31,33,60,29,32,50,28,44,34,33,20,37,11,6,12,20,8,8,13,31,10,8,8,14,8,5,7,20,13,7,6,11,3,3,3,3,3,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,27,24,14,29,20,27,26,20,23,21,36,7,18,11,27,17,25,7,14,24,4,4,3,4,4,3,3,5,4,3,4,5,2,5,6,2,5,5,6,3,4,4,3,4,3,3,4,5,3,4,3,4,2,3,6,3,5,5,4,5,4,4,4,5,3,4,4,5,4,4,2,5,2,5,6,3,5,3,5,6,5,5,5,6,4,5,4,6,6,5,3,6,2,4,8,4,5,2,4,6,8,7,9,8,6,7,8,8,7,9,8,7,7,7,7,6,7,4,4,8,4,6,4,5,3,3,5,5,4,4,2,4,2,2,6,4,5,2,3,5,4,5,4,4,3,3,3,4,3,4,2,4,3,3,5,4,3,2,3,5,5,3,4,4,3,5,4,3,2,4,4,3,2,2,3,3,3,3,3,4,4,2,3,4,3,2,2,4,2,2,3,2,3,3,4,2,3,2,5,3,4,2,2,2,3,2,2,5,2,2,3,2,3,2,2,2,2,2,2,2,4,3,3,4,3,4,4,4,2,3,2,3,2,2,2,3,3,2,2,3,2,2,3,3,3,2,2,4,2,3,3,2,2,3,2,2,4,2,2,4,2,4,3,4,2,3,3,4,2,3,4,2,2,2,3,3,4,3,3,4,7,5,7,8,6,8,7,7,6,8,7,6,6,7,6,6,4,5,4,7,4,5,4,5,3,3,4,5,3,4,4,5,3,4,5,5,5,3,2,5,2,4,4,5,3,4,4,4,4,4,2,4,2,4,7,5,5,3,3,6,4,3,4,5,2,3,4,4,4,3,3,3,2,3,5,4,4,2,4,4,4,3,5,5,5,4,4,4,3,3,4,4,3,4,5,4,5,4,2,4,2,3,2,4,3,2,2,2,3,2,2,3,3,2,4,2,3,2,3,2,2,3,2,4,3,2,2,2,3,2,2,2,2,2,3,2,3,3,3,2,2,3,2,3,4,2,2,2,3,2,2,2,2,3,3,2,3,3,3,2,2,2,2,3,4,2,3,2,3,2,3,2,3,3,4,3,3,3,3,2,2,2,2,2,3,2,2,2,3,2,2,2,2,2,3,3,2,2,3,2))
    //     // Print errors in case they happened
    //     if (!excepts.isEmpty) {
    //       println("Exceptions found during parsing:")
    //       excepts.foreach(println)
    //     }
    //     // if(!excepts.isEmpty) {

    //     //   outputInfoLine("ERROR: PARSING EXCEPTIONS WHERE CATCHED:")

    //     //   excepts.foreach{ case (line, except) =>
    //     //     val message = except.getMessage()
    //     //     outputInfoLine(s"Line = $line")
    //     //     outputInfoLine(s"Exception = $message")
    //     //   }
    //     // }
    //     df1
    //   case "libsvm" => 
    //     readSVMToDF(args(0), args(2).toInt, Array(args(3), args(4)))
    //   case "parquet" =>
    //     spark.read.parquet(args(0))
    // }

    // // CFS Model

    // // Gets the datasets basename
    // // val baseName = args(0).split('_').head.split('/').last
    // // // Ex.: /root/ECBDL14_k10m40_feats_weights.txt
    // // val basePath = args(1) + "/" + baseName + "_k" + args(2) + "m" + args(3) + "ramp" + args(5)

    // // CFS Feature Selection
    // // args(0) Dataset full location
    // // val attrs


    // // df.select(attrs)

    // def discretizeDF(df: DataFrame, nColumns: Int, nBinsPerColumn: Int)
    //   : DataFrame = {

    //   def discretizeNominal(idx: Int): DataFrame = {
    //     // Try to discretize everything except the label
    //     if(idx < nColumns - 1){
    //       if(attrs(idx).isNumeric){
    //         val name = attrs(idx).name.get
    //         val discretizer = (new QuantileDiscretizer()
    //           .setNumBuckets(nBinsPerColumn(idx))
    //           .setInputCol(name)
    //           .setOutputCol(name + "-discretized")
    //         )
    //         // Test if caching if needed here!
    //         discretizeNominal(
    //           discretizer.fit(df).transform(df).cache, idx + 1)
    //       } else discretizeNominal(df, idx + 1)
    //     } else df
    //   }

    //   discretizeNominal(0)
    // }

    // val reducedDf = df.select(attrsNames(0), attrsNames.slice(1,316):_*).persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY)

    // df.write.format("parquet").save(args(0).split('.').head + ".parquet")



val nBinsPerColumn=Array(9,11,32,4,4,4,4,3,4,4,4,4,4,4,4,4,3,4,4,4,4,12,12,22,21,22,11,27,17,21,28,13,25,6,18,8,19,26,15,6,6,6,6,5,6,6,6,6,6,6,6,6,5,6,6,6,6,6,6,6,6,5,6,6,6,6,6,6,6,6,5,6,6,6,6,6,6,6,6,5,6,6,6,6,6,6,6,6,5,6,6,6,6,40,46,60,27,69,31,31,33,60,29,32,50,28,44,34,33,20,37,11,6,12,20,8,8,13,31,10,8,8,14,8,5,7,20,13,7,6,11,3,3,3,3,3,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,27,24,14,29,20,27,26,20,23,21,36,7,18,11,27,17,25,7,14,24,4,4,3,4,4,3,3,5,4,3,4,5,2,5,6,2,5,5,6,3,4,4,3,4,3,3,4,5,3,4,3,4,2,3,6,3,5,5,4,5,4,4,4,5,3,4,4,5,4,4,2,5,2,5,6,3,5,3,5,6,5,5,5,6,4,5,4,6,6,5,3,6,2,4,8,4,5,2,4,6,8,7,9,8,6,7,8,8,7,9,8,7,7,7,7,6,7,4,4,8,4,6,4,5,3,3,5,5,4,4,2,4,2,2,6,4,5,2,3,5,4,5,4,4,3,3,3,4,3,4,2,4,3,3,5,4,3,2,3,5,5,3,4,4,3,5,4,3,2,4,4,3,2,2,3,3,3,3,3,4,4,2,3,4,3,2,2,4,2,2,3,2,3,3,4,2,3,2,5,3,4,2,2,2,3,2,2,5,2,2,3,2,3,2,2,2,2,2,2,2,4,3,3,4,3,4,4,4,2,3,2,3,2,2,2,3,3,2,2,3,2,2,3,3,3,2,2,4,2,3,3,2,2,3,2,2,4,2,2,4,2,4,3,4,2,3,3,4,2,3,4,2,2,2,3,3,4,3,3,4,7,5,7,8,6,8,7,7,6,8,7,6,6,7,6,6,4,5,4,7,4,5,4,5,3,3,4,5,3,4,4,5,3,4,5,5,5,3,2,5,2,4,4,5,3,4,4,4,4,4,2,4,2,4,7,5,5,3,3,6,4,3,4,5,2,3,4,4,4,3,3,3,2,3,5,4,4,2,4,4,4,3,5,5,5,4,4,4,3,3,4,4,3,4,5,4,5,4,2,4,2,3,2,4,3,2,2,2,3,2,2,3,3,2,4,2,3,2,3,2,2,3,2,4,3,2,2,2,3,2,2,2,2,2,3,2,3,3,3,2,2,3,2,3,4,2,2,2,3,2,2,2,2,3,3,2,3,3,3,2,2,2,2,3,4,2,3,2,3,2,3,2,3,3,4,3,3,3,3,2,2,2,2,2,3,2,2,2,3,2,2,2,2,2,3,3,2,2,3,2)

val attrsTypes = Array(true, true, true, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true)

val attrsNames=Array("separation","propensity","length","PredSS_r1_-4","PredSS_r1_-3","PredSS_r1_-2","PredSS_r1_-1","PredSS_r1","PredSS_r1_1","PredSS_r1_2","PredSS_r1_3","PredSS_r1_4","PredSS_r2_-4","PredSS_r2_-3","PredSS_r2_-2","PredSS_r2_-1","PredSS_r2","PredSS_r2_1","PredSS_r2_2","PredSS_r2_3","PredSS_r2_4","PredSS_freq_central_H","PredSS_freq_central_E","PredSS_freq_central_C","PredCN_freq_central_0","PredCN_freq_central_1","PredCN_freq_central_2","PredCN_freq_central_3","PredCN_freq_central_4","PredRCH_freq_central_0","PredRCH_freq_central_1","PredRCH_freq_central_2","PredRCH_freq_central_3","PredRCH_freq_central_4","PredSA_freq_central_0","PredSA_freq_central_1","PredSA_freq_central_2","PredSA_freq_central_3","PredSA_freq_central_4","PredRCH_r1_-4","PredRCH_r1_-3","PredRCH_r1_-2","PredRCH_r1_-1","PredRCH_r1","PredRCH_r1_1","PredRCH_r1_2","PredRCH_r1_3","PredRCH_r1_4","PredRCH_r2_-4","PredRCH_r2_-3","PredRCH_r2_-2","PredRCH_r2_-1","PredRCH_r2","PredRCH_r2_1","PredRCH_r2_2","PredRCH_r2_3","PredRCH_r2_4","PredCN_r1_-4","PredCN_r1_-3","PredCN_r1_-2","PredCN_r1_-1","PredCN_r1","PredCN_r1_1","PredCN_r1_2","PredCN_r1_3","PredCN_r1_4","PredCN_r2_-4","PredCN_r2_-3","PredCN_r2_-2","PredCN_r2_-1","PredCN_r2","PredCN_r2_1","PredCN_r2_2","PredCN_r2_3","PredCN_r2_4","PredSA_r1_-4","PredSA_r1_-3","PredSA_r1_-2","PredSA_r1_-1","PredSA_r1","PredSA_r1_1","PredSA_r1_2","PredSA_r1_3","PredSA_r1_4","PredSA_r2_-4","PredSA_r2_-3","PredSA_r2_-2","PredSA_r2_-1","PredSA_r2","PredSA_r2_1","PredSA_r2_2","PredSA_r2_3","PredSA_r2_4","PredSS_freq_global_H","PredSS_freq_global_E","PredSS_freq_global_C","PredCN_freq_global_0","PredCN_freq_global_1","PredCN_freq_global_2","PredCN_freq_global_3","PredCN_freq_global_4","PredRCH_freq_global_0","PredRCH_freq_global_1","PredRCH_freq_global_2","PredRCH_freq_global_3","PredRCH_freq_global_4","PredSA_freq_global_0","PredSA_freq_global_1","PredSA_freq_global_2","PredSA_freq_global_3","PredSA_freq_global_4","AA_freq_central_A","AA_freq_central_R","AA_freq_central_N","AA_freq_central_D","AA_freq_central_C","AA_freq_central_Q","AA_freq_central_E","AA_freq_central_G","AA_freq_central_H","AA_freq_central_I","AA_freq_central_L","AA_freq_central_K","AA_freq_central_M","AA_freq_central_F","AA_freq_central_P","AA_freq_central_S","AA_freq_central_T","AA_freq_central_W","AA_freq_central_Y","AA_freq_central_V","PredSS_central_-2","PredSS_central_-1","PredSS_central","PredSS_central_1","PredSS_central_2","PredCN_central_-2","PredCN_central_-1","PredCN_central","PredCN_central_1","PredCN_central_2","PredRCH_central_-2","PredRCH_central_-1","PredRCH_central","PredRCH_central_1","PredRCH_central_2","PredSA_central_-2","PredSA_central_-1","PredSA_central","PredSA_central_1","PredSA_central_2","AA_freq_global_A","AA_freq_global_R","AA_freq_global_N","AA_freq_global_D","AA_freq_global_C","AA_freq_global_Q","AA_freq_global_E","AA_freq_global_G","AA_freq_global_H","AA_freq_global_I","AA_freq_global_L","AA_freq_global_K","AA_freq_global_M","AA_freq_global_F","AA_freq_global_P","AA_freq_global_S","AA_freq_global_T","AA_freq_global_W","AA_freq_global_Y","AA_freq_global_V","PSSM_r1_-4_A","PSSM_r1_-4_R","PSSM_r1_-4_N","PSSM_r1_-4_D","PSSM_r1_-4_C","PSSM_r1_-4_Q","PSSM_r1_-4_E","PSSM_r1_-4_G","PSSM_r1_-4_H","PSSM_r1_-4_I","PSSM_r1_-4_L","PSSM_r1_-4_K","PSSM_r1_-4_M","PSSM_r1_-4_F","PSSM_r1_-4_P","PSSM_r1_-4_S","PSSM_r1_-4_T","PSSM_r1_-4_W","PSSM_r1_-4_Y","PSSM_r1_-4_V","PSSM_r1_-3_A","PSSM_r1_-3_R","PSSM_r1_-3_N","PSSM_r1_-3_D","PSSM_r1_-3_C","PSSM_r1_-3_Q","PSSM_r1_-3_E","PSSM_r1_-3_G","PSSM_r1_-3_H","PSSM_r1_-3_I","PSSM_r1_-3_L","PSSM_r1_-3_K","PSSM_r1_-3_M","PSSM_r1_-3_F","PSSM_r1_-3_P","PSSM_r1_-3_S","PSSM_r1_-3_T","PSSM_r1_-3_W","PSSM_r1_-3_Y","PSSM_r1_-3_V","PSSM_r1_-2_A","PSSM_r1_-2_R","PSSM_r1_-2_N","PSSM_r1_-2_D","PSSM_r1_-2_C","PSSM_r1_-2_Q","PSSM_r1_-2_E","PSSM_r1_-2_G","PSSM_r1_-2_H","PSSM_r1_-2_I","PSSM_r1_-2_L","PSSM_r1_-2_K","PSSM_r1_-2_M","PSSM_r1_-2_F","PSSM_r1_-2_P","PSSM_r1_-2_S","PSSM_r1_-2_T","PSSM_r1_-2_W","PSSM_r1_-2_Y","PSSM_r1_-2_V","PSSM_r1_-1_A","PSSM_r1_-1_R","PSSM_r1_-1_N","PSSM_r1_-1_D","PSSM_r1_-1_C","PSSM_r1_-1_Q","PSSM_r1_-1_E","PSSM_r1_-1_G","PSSM_r1_-1_H","PSSM_r1_-1_I","PSSM_r1_-1_L","PSSM_r1_-1_K","PSSM_r1_-1_M","PSSM_r1_-1_F","PSSM_r1_-1_P","PSSM_r1_-1_S","PSSM_r1_-1_T","PSSM_r1_-1_W","PSSM_r1_-1_Y","PSSM_r1_-1_V","PSSM_r1_0_A","PSSM_r1_0_R","PSSM_r1_0_N","PSSM_r1_0_D","PSSM_r1_0_C","PSSM_r1_0_Q","PSSM_r1_0_E","PSSM_r1_0_G","PSSM_r1_0_H","PSSM_r1_0_I","PSSM_r1_0_L","PSSM_r1_0_K","PSSM_r1_0_M","PSSM_r1_0_F","PSSM_r1_0_P","PSSM_r1_0_S","PSSM_r1_0_T","PSSM_r1_0_W","PSSM_r1_0_Y","PSSM_r1_0_V","PSSM_r1_1_A","PSSM_r1_1_R","PSSM_r1_1_N","PSSM_r1_1_D","PSSM_r1_1_C","PSSM_r1_1_Q","PSSM_r1_1_E","PSSM_r1_1_G","PSSM_r1_1_H","PSSM_r1_1_I","PSSM_r1_1_L","PSSM_r1_1_K","PSSM_r1_1_M","PSSM_r1_1_F","PSSM_r1_1_P","PSSM_r1_1_S","PSSM_r1_1_T","PSSM_r1_1_W","PSSM_r1_1_Y","PSSM_r1_1_V","PSSM_r1_2_A","PSSM_r1_2_R","PSSM_r1_2_N","PSSM_r1_2_D","PSSM_r1_2_C","PSSM_r1_2_Q","PSSM_r1_2_E","PSSM_r1_2_G","PSSM_r1_2_H","PSSM_r1_2_I","PSSM_r1_2_L","PSSM_r1_2_K","PSSM_r1_2_M","PSSM_r1_2_F","PSSM_r1_2_P","PSSM_r1_2_S","PSSM_r1_2_T","PSSM_r1_2_W","PSSM_r1_2_Y","PSSM_r1_2_V","PSSM_r1_3_A","PSSM_r1_3_R","PSSM_r1_3_N","PSSM_r1_3_D","PSSM_r1_3_C","PSSM_r1_3_Q","PSSM_r1_3_E","PSSM_r1_3_G","PSSM_r1_3_H","PSSM_r1_3_I","PSSM_r1_3_L","PSSM_r1_3_K","PSSM_r1_3_M","PSSM_r1_3_F","PSSM_r1_3_P","PSSM_r1_3_S","PSSM_r1_3_T","PSSM_r1_3_W","PSSM_r1_3_Y","PSSM_r1_3_V","PSSM_r1_4_A","PSSM_r1_4_R","PSSM_r1_4_N","PSSM_r1_4_D","PSSM_r1_4_C","PSSM_r1_4_Q","PSSM_r1_4_E","PSSM_r1_4_G","PSSM_r1_4_H","PSSM_r1_4_I","PSSM_r1_4_L","PSSM_r1_4_K","PSSM_r1_4_M","PSSM_r1_4_F","PSSM_r1_4_P","PSSM_r1_4_S","PSSM_r1_4_T","PSSM_r1_4_W","PSSM_r1_4_Y","PSSM_r1_4_V","PSSM_r2_-4_A","PSSM_r2_-4_R","PSSM_r2_-4_N","PSSM_r2_-4_D","PSSM_r2_-4_C","PSSM_r2_-4_Q","PSSM_r2_-4_E","PSSM_r2_-4_G","PSSM_r2_-4_H","PSSM_r2_-4_I","PSSM_r2_-4_L","PSSM_r2_-4_K","PSSM_r2_-4_M","PSSM_r2_-4_F","PSSM_r2_-4_P","PSSM_r2_-4_S","PSSM_r2_-4_T","PSSM_r2_-4_W","PSSM_r2_-4_Y","PSSM_r2_-4_V","PSSM_r2_-3_A","PSSM_r2_-3_R","PSSM_r2_-3_N","PSSM_r2_-3_D","PSSM_r2_-3_C","PSSM_r2_-3_Q","PSSM_r2_-3_E","PSSM_r2_-3_G","PSSM_r2_-3_H","PSSM_r2_-3_I","PSSM_r2_-3_L","PSSM_r2_-3_K","PSSM_r2_-3_M","PSSM_r2_-3_F","PSSM_r2_-3_P","PSSM_r2_-3_S","PSSM_r2_-3_T","PSSM_r2_-3_W","PSSM_r2_-3_Y","PSSM_r2_-3_V","PSSM_r2_-2_A","PSSM_r2_-2_R","PSSM_r2_-2_N","PSSM_r2_-2_D","PSSM_r2_-2_C","PSSM_r2_-2_Q","PSSM_r2_-2_E","PSSM_r2_-2_G","PSSM_r2_-2_H","PSSM_r2_-2_I","PSSM_r2_-2_L","PSSM_r2_-2_K","PSSM_r2_-2_M","PSSM_r2_-2_F","PSSM_r2_-2_P","PSSM_r2_-2_S","PSSM_r2_-2_T","PSSM_r2_-2_W","PSSM_r2_-2_Y","PSSM_r2_-2_V","PSSM_r2_-1_A","PSSM_r2_-1_R","PSSM_r2_-1_N","PSSM_r2_-1_D","PSSM_r2_-1_C","PSSM_r2_-1_Q","PSSM_r2_-1_E","PSSM_r2_-1_G","PSSM_r2_-1_H","PSSM_r2_-1_I","PSSM_r2_-1_L","PSSM_r2_-1_K","PSSM_r2_-1_M","PSSM_r2_-1_F","PSSM_r2_-1_P","PSSM_r2_-1_S","PSSM_r2_-1_T","PSSM_r2_-1_W","PSSM_r2_-1_Y","PSSM_r2_-1_V","PSSM_r2_0_A","PSSM_r2_0_R","PSSM_r2_0_N","PSSM_r2_0_D","PSSM_r2_0_C","PSSM_r2_0_Q","PSSM_r2_0_E","PSSM_r2_0_G","PSSM_r2_0_H","PSSM_r2_0_I","PSSM_r2_0_L","PSSM_r2_0_K","PSSM_r2_0_M","PSSM_r2_0_F","PSSM_r2_0_P","PSSM_r2_0_S","PSSM_r2_0_T","PSSM_r2_0_W","PSSM_r2_0_Y","PSSM_r2_0_V","PSSM_r2_1_A","PSSM_r2_1_R","PSSM_r2_1_N","PSSM_r2_1_D","PSSM_r2_1_C","PSSM_r2_1_Q","PSSM_r2_1_E","PSSM_r2_1_G","PSSM_r2_1_H","PSSM_r2_1_I","PSSM_r2_1_L","PSSM_r2_1_K","PSSM_r2_1_M","PSSM_r2_1_F","PSSM_r2_1_P","PSSM_r2_1_S","PSSM_r2_1_T","PSSM_r2_1_W","PSSM_r2_1_Y","PSSM_r2_1_V","PSSM_r2_2_A","PSSM_r2_2_R","PSSM_r2_2_N","PSSM_r2_2_D","PSSM_r2_2_C","PSSM_r2_2_Q","PSSM_r2_2_E","PSSM_r2_2_G","PSSM_r2_2_H","PSSM_r2_2_I","PSSM_r2_2_L","PSSM_r2_2_K","PSSM_r2_2_M","PSSM_r2_2_F","PSSM_r2_2_P","PSSM_r2_2_S","PSSM_r2_2_T","PSSM_r2_2_W","PSSM_r2_2_Y","PSSM_r2_2_V","PSSM_r2_3_A","PSSM_r2_3_R","PSSM_r2_3_N","PSSM_r2_3_D","PSSM_r2_3_C","PSSM_r2_3_Q","PSSM_r2_3_E","PSSM_r2_3_G","PSSM_r2_3_H","PSSM_r2_3_I","PSSM_r2_3_L","PSSM_r2_3_K","PSSM_r2_3_M","PSSM_r2_3_F","PSSM_r2_3_P","PSSM_r2_3_S","PSSM_r2_3_T","PSSM_r2_3_W","PSSM_r2_3_Y","PSSM_r2_3_V","PSSM_r2_4_A","PSSM_r2_4_R","PSSM_r2_4_N","PSSM_r2_4_D","PSSM_r2_4_C","PSSM_r2_4_Q","PSSM_r2_4_E","PSSM_r2_4_G","PSSM_r2_4_H","PSSM_r2_4_I","PSSM_r2_4_L","PSSM_r2_4_K","PSSM_r2_4_M","PSSM_r2_4_F","PSSM_r2_4_P","PSSM_r2_4_S","PSSM_r2_4_T","PSSM_r2_4_W","PSSM_r2_4_Y","PSSM_r2_4_V","PSSM_central_-2_A","PSSM_central_-2_R","PSSM_central_-2_N","PSSM_central_-2_D","PSSM_central_-2_C","PSSM_central_-2_Q","PSSM_central_-2_E","PSSM_central_-2_G","PSSM_central_-2_H","PSSM_central_-2_I","PSSM_central_-2_L","PSSM_central_-2_K","PSSM_central_-2_M","PSSM_central_-2_F","PSSM_central_-2_P","PSSM_central_-2_S","PSSM_central_-2_T","PSSM_central_-2_W","PSSM_central_-2_Y","PSSM_central_-2_V","PSSM_central_-1_A","PSSM_central_-1_R","PSSM_central_-1_N","PSSM_central_-1_D","PSSM_central_-1_C","PSSM_central_-1_Q","PSSM_central_-1_E","PSSM_central_-1_G","PSSM_central_-1_H","PSSM_central_-1_I","PSSM_central_-1_L","PSSM_central_-1_K","PSSM_central_-1_M","PSSM_central_-1_F","PSSM_central_-1_P","PSSM_central_-1_S","PSSM_central_-1_T","PSSM_central_-1_W","PSSM_central_-1_Y","PSSM_central_-1_V","PSSM_central_0_A","PSSM_central_0_R","PSSM_central_0_N","PSSM_central_0_D","PSSM_central_0_C","PSSM_central_0_Q","PSSM_central_0_E","PSSM_central_0_G","PSSM_central_0_H","PSSM_central_0_I","PSSM_central_0_L","PSSM_central_0_K","PSSM_central_0_M","PSSM_central_0_F","PSSM_central_0_P","PSSM_central_0_S","PSSM_central_0_T","PSSM_central_0_W","PSSM_central_0_Y","PSSM_central_0_V","PSSM_central_1_A","PSSM_central_1_R","PSSM_central_1_N","PSSM_central_1_D","PSSM_central_1_C","PSSM_central_1_Q","PSSM_central_1_E","PSSM_central_1_G","PSSM_central_1_H","PSSM_central_1_I","PSSM_central_1_L","PSSM_central_1_K","PSSM_central_1_M","PSSM_central_1_F","PSSM_central_1_P","PSSM_central_1_S","PSSM_central_1_T","PSSM_central_1_W","PSSM_central_1_Y","PSSM_central_1_V","PSSM_central_2_A","PSSM_central_2_R","PSSM_central_2_N","PSSM_central_2_D","PSSM_central_2_C","PSSM_central_2_Q","PSSM_central_2_E","PSSM_central_2_G","PSSM_central_2_H","PSSM_central_2_I","PSSM_central_2_L","PSSM_central_2_K","PSSM_central_2_M","PSSM_central_2_F","PSSM_central_2_P","PSSM_central_2_S","PSSM_central_2_T","PSSM_central_2_W","PSSM_central_2_Y","PSSM_central_2_V")
def discretizeDF(df: DataFrame, lstart: Int, lend: Int)
  : DataFrame = {

  def discretizeNominal(df: DataFrame, idx: Int): DataFrame = {
    // Try to discretize everything except the label
    // if(idx < nColumns - 1){
    if(idx < lend){
      // if(attrs(idx).isNumeric){
      if(attrsTypes(idx)){
        // val name = attrs(idx).name.get
        val name = attrsNames(idx)
        val discretizer = (new QuantileDiscretizer()
          .setNumBuckets(nBinsPerColumn(idx))
          .setInputCol(name)
          .setOutputCol(name + "-discretized")
        )
        // Test if caching if needed here!
        discretizeNominal(discretizer.fit(df).transform(df), idx + 1)
      } else discretizeNominal(df, idx + 1)
    } else df
  }

  discretizeNominal(df, lstart)
}



val limits = Array(0,161,231,301,371,441,511,581,630)
// val df = spark.read.parquet(args(0)).persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY)
val df = spark.read.parquet(args(0))

for( i <- 0 to limits.size - 2) {

  val lstart = limits(i)
  val lend = limits(i + 1)
  
  val reducedDF = df.select(attrsNames(lstart), attrsNames.slice(lstart + 1,lend):_*)

  val discretizedDF = discretizeDF(reducedDF, lstart, lend)
  discretizedDF.write.format("parquet").save(args(0).split('.').head + "discretized" + lstart.toString + "-" + (lend-1).toString + ".parquet")
  

}





















    // val metadata: Metadata = ...
    // df.select($"colA".as("colB", metadata))

    // val ag = AttributeGroup.fromStructField(discreteDF.schema("features"))
    // val attrs: Array[Attribute] = ag.attributes.get
    // println(attrs.map{_.index.get}.mkString(","))
    // println(
    //   discreteDF.collect.map{
    //     case Row(features: Vector, label: Double) =>
    //       (features.toDense, label)
    //   }.mkString("\n")
    // )

   
    // Transform DataFrame to RDD[LabeledPoint]
    // val data: RDD[LabeledPoint] = 
    //   df.select("features", "label").map {
    //   case Row(features: Vector, label: Double) =>
    //     LabeledPoint(label, features)
    //   }
    // val featureSelector = new CfsFeatureSelector(data)
    // val feats: BitSet = 
    //   featureSelector.searchFeaturesSubset(
    //     args(1).stripPrefix("useLocallyPred=").toBoolean,
    //     args(2).stripPrefix("useGA=").toBoolean,
    //     args(3).stripPrefix("usePopGTEnFeats=").toBoolean,
    //     args(4).stripPrefix("optIslandPopulationSize=").toInt)

    // println("SELECTED FEATS = " + feats.toString)

    // println("Weights:")
    // model.featuresWeights.zipWithIndex.foreach{ case (w,i) => 
    //   println(s"$w, ${i+1}")
    // }
    // val reducedDf = model.transform(df)


    // Classifier
    // args(0) _train.parquet
    // args(1) ECBDL14_feats_knn.txt

    // import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier, DecisionTreeClassifier, MultilayerPerceptronClassifier}
    // import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
    // import org.apache.spark.ml.feature.VectorSlicer

    // val dfTrain = df
    // val dfTest = spark.read.parquet(args(0).split('_').head + "_test.parquet")

    // // read selectedFeats from file
    // var dfReducedTrain: DataFrame = null 
    // var dfReducedTest: DataFrame = null
    // if (args(1) != "full"){
    //   import scala.io.Source
    //   println("Reading feats from file: " + args(1).split('/').last)
    //   val selectedFeatures = Source.fromFile(args(1)).getLines.toArray.map(_.toInt)

    //   def dfReducer(df: DataFrame, selectedFeatures: Array[Int]): DataFrame = {
    //     var slicer = new VectorSlicer().setInputCol("features").setOutputCol("selectedFeatures")
    //     slicer.setIndices(selectedFeatures)
    //     // Return reduced DataFrame
    //     (slicer
    //       .transform(df)
    //       .selectExpr("selectedFeatures as features", "label")
    //     )
    //   }
    //   dfReducedTrain = dfReducer(dfTrain, selectedFeatures)
    //   dfReducedTest = dfReducer(dfTest, selectedFeatures)

    // } else {
      
    //   dfReducedTrain = dfTrain
    //   dfReducedTest = dfTest

    // }

    // args(2) match {
    //   // case "DecisionTree" =>
    //   //   new DecisionTreeClassifier()

    //   case "RandomForest" =>
    //       val classifier = new RandomForestClassifier()//.setNumTrees(10)
    //       val classifierModel = classifier.fit(dfReducedTrain)
    //       val predictions = classifierModel.transform(dfReducedTest)

    //       val evaluator = (new MulticlassClassificationEvaluator()
    //                             .setMetricName("precision"))

    //       val precision = evaluator.evaluate(predictions)

    //       println(s"${args(2)} precision is: " + precision.toString)

    
    //   case "NaiveBayes" =>
    //     import org.apache.spark.mllib.classification.NaiveBayes
    //     import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
    //     import org.apache.spark.mllib.linalg.Vector
    //     import org.apache.spark.mllib.linalg.Vectors

    //     // Convert to RDD
    //     val LPTrain: RDD[LabeledPoint] = 
    //       dfReducedTrain.select("features", "label").map {
    //       case Row(features: Vector, label: Double) =>
    //         LabeledPoint(label, Vectors.dense(features.toArray.map(_ + 1000)))
    //     }
    //     val LPTest: RDD[LabeledPoint] = 
    //       dfReducedTest.select("features", "label").map {
    //       case Row(features: Vector, label: Double) =>
    //         LabeledPoint(label, Vectors.dense(features.toArray.map(_ + 1000)))
    //     }
    //     val model = NaiveBayes.train(LPTrain, lambda = 1.0, modelType = "multinomial")
    //     val predictionAndLabel = LPTest.map(p => (model.predict(p.features), p.label))
    //     // Evaluate
    //     val evaluator = new BinaryClassificationMetrics(predictionAndLabel)
    //     println(s"${args(2)} AUC is: " + evaluator.areaUnderROC)
    //     // .setLayers(layers)
    //     // .setBlockSize(128)
    //     // .setSeed(1234L)
    //     // .setMaxIter(100)
    // }


    // DataFrame Saver

    // val subdf = 
    //   (if (args(1) != "1.0") 
    //     df.sample(withReplacement=false, fraction=args(1).toDouble) 
    //    else df)
    // // Save to parquet
    // subdf.write.format("parquet").save(args(0).split('.').head + "_" + (args(1).toDouble * 100.0).toInt.toString + "perc.parquet")


    // DataFrame Merger

    // val df2 = spark.read.parquet(args(1))
    // val dfTot = df.unionAll(df2)
    // dfTot.write.format("parquet").save(args(0) + "merged.parquet")

    // Other things

    // val data = parseNominalCSVtoLabeledPoint(sc.textFile(fLocation))
    // data.cache()

    // val subset = 
    //   new CfsFeatureSelector(data, sc).searchFeaturesSubset(
    //     addLocalFeats = false)

    // println("The selected features are:")
    // subset.foreach(println)

  }
}