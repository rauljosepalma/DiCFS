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

import scala.collection.Map
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
  def readArffToDF(fLocation: String): 
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

  // Returns a new DataFrame with numeric features discretized
  // Observe the following: 
  //   - Binary feats are not supported.
  //   - Features order is lost, nominal features will come first.
  //   - Original numeric features' names will be lost.
  //   - New nominal features will not contain metadata about their values.
  def discretizeDF(data:DataFrame, 
    featuresCol:String = "features", labelCol:String = "label"): DataFrame = {

     // Extract attributes from metadata.
    val ag = AttributeGroup.fromStructField(data.schema(featuresCol))
    val attrs: Array[Attribute] = ag.attributes.get
    val numericFeats: Array[Int] = (attrs
        .filter(_.attrType == AttributeType.Numeric)
        .map(_.index.get)
      )
    val nominalFeats: Array[Int] = (attrs
        .filter(_.attrType == AttributeType.Nominal)
        .map(_.index.get)
      )

    // Separate feats two new columns
    val slicerNumeric = (new VectorSlicer()
      .setInputCol(featuresCol)
      .setOutputCol(featuresCol + "-numeric")
      .setIndices(numericFeats))
    val slicerNominal = (new VectorSlicer()
      .setInputCol(featuresCol)
      .setOutputCol(featuresCol + "-nominal")
      .setIndices(nominalFeats))
    
    val dataSeparatedFeats: DataFrame = 
      slicerNominal.transform(slicerNumeric.transform(data))
    
    val discretizer = (new MDLPDiscretizer()
      .setMaxBins(20)
      .setMaxByPart(1000)
      .setInputCol(featuresCol + "-numeric")
      .setLabelCol(labelCol)
      .setOutputCol(featuresCol + "-discretized")
    )

    // Discretize numeric feats column and drop irrelevant cols
    val dfDiscretized: DataFrame = (discretizer
      .fit(dataSeparatedFeats).transform(dataSeparatedFeats)
      .select(featuresCol+"-nominal", featuresCol+"-discretized", labelCol)
    )

    // merge discretized feats with nominal feats
    val assembler = (new VectorAssembler()
      .setInputCols(Array(featuresCol+"-nominal", featuresCol+"-discretized"))
      .setOutputCol(featuresCol))

    assembler.transform(dfDiscretized).select(featuresCol, labelCol)

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
    
    val df = args(0).split('.').last match {
      case "arff" => 
        val (df1, excepts) = readArffToDF(args(0))
        // Print errors in case they happened
        if (!excepts.isEmpty) {
          println("Exceptions found during parsing:")
          excepts.foreach(println)
        }
        // if(!excepts.isEmpty) {

        //   outputInfoLine("ERROR: PARSING EXCEPTIONS WHERE CATCHED:")

        //   excepts.foreach{ case (line, except) =>
        //     val message = except.getMessage()
        //     outputInfoLine(s"Line = $line")
        //     outputInfoLine(s"Exception = $message")
        //   }
        // }
        df1
      case "libsvm" => 
        readSVMToDF(args(0), args(2).toInt, Array(args(3), args(4)))
      case "parquet" =>
        spark.read.parquet(args(0))
    }

    // CFS Model

    // Gets the datasets basename
    // val baseName = args(0).split('_').head.split('/').last
    // // Ex.: /root/ECBDL14_k10m40_feats_weights.txt
    // val basePath = args(1) + "/" + baseName + "_k" + args(2) + "m" + args(3) + "ramp" + args(5)

    // CFS Feature Selection
    // args(0) Dataset full location

    // Discretize & save dataset
    df.cache
    // println("Doing nothing right now!")
    val discreteDF = discretizeDF(df)
    df.write.format("parquet").save(args(0).split('.').head + "-discrete-no_meta.parquet")

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