package org.apache.spark.ml.feature

import org.apache.spark.SparkContext
import org.apache.spark.SparkException
import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.attribute._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.util.CollectionAccumulator

import scala.collection.mutable
import scala.collection.mutable.Buffer
import scala.collection.immutable.IndexedSeq
import scala.collection.immutable.BitSet
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

// import org.apache.spark.mllib.feature.MDLPDiscretizer
import rauljosepalma.sparkmltools._


object Main {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("spark-cfs")
    val sc = SparkContext.getOrCreate
    val spark = SparkSession.builder().getOrCreate()
    
    // Reduce verbosity
    sc.setLogLevel("WARN")
    
    val df = args(0).split('.').last match {
      case "arff" => 
        val (df1, excepts) = DataFrameIO.readDFFromArff(args(0))
        // Print errors in case they happened
        if (!excepts.isEmpty) {
          println("Exceptions found during parsing:")
          excepts.foreach(println)
        }
        df1
      case "libsvm" => 
        DataFrameIO.readDFFromSVM(
            args(0), args(2).toInt, Array(args(3), args(4)))
      case "parquet" =>
        spark.read.parquet(args(0))
    }

    // // CFS Model

    // // Gets the datasets basename
    // // val baseName = args(0).split('_').head.split('/').last
    // // // Ex.: /root/ECBDL14_k10m40_feats_weights.txt
    // // val basePath = args(1) + "/" + baseName + "_k" + args(2) + "m" + args(3) + "ramp" + args(5)

    // // CFS Feature Selection
    // // args(0) Dataset full location
    val featureSelector = new CfsFeatureSelector
    val feats: BitSet = 
      featureSelector.fit(
        df,
        args(1).stripPrefix("useLocallyPred=").toBoolean,
        args(2).stripPrefix("useGA=").toBoolean,
        args(3).stripPrefix("useNFeatsForPopulationSize=").toBoolean,
        args(4).stripPrefix("optIslandPopulationSize=").toInt)

    println("SELECTED FEATS = " + feats.toString)

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

  }
}