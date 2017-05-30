package org.apache.spark.ml.feature

import org.apache.spark.SparkContext
import org.apache.spark.SparkException
import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.attribute._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}


// import org.apache.spark.mllib.feature.MDLPDiscretizer
import rauljosepalma.sparkmltools._


object Main {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("spark-cfs")
    val sc = SparkContext.getOrCreate
    val spark = SparkSession.builder().getOrCreate()
    
    // Reduce verbosity
    sc.setLogLevel("WARN")

    import org.apache.spark.storage.StorageLevel    
    val df = DataFrameIO.readDFFromAny(args(0))
    // val df = DataFrameIO.readDFFromAny(args(0)).persist(StorageLevel.MEMORY_ONLY)
    // ex.: ECBDL14_train
    val dfName = args(0).split('/').last.split('.').head

    // ex.: hdfs://master:8020/datasets
    // val dfPath = args(0).slice(0, args(0).lastIndexOf("/"))
    // Note that this basePath is for @master
    // val resultsPath = args(1)
    // val resultsFileBasePath = 
    //   args(1).stripPrefix("resultsPath=") + dfName + "_" +
    //   args.slice(2,args.size).mkString("_")

    val sampleSize = args(1).stripPrefix("sampleSize=").toDouble

    println(s"DATASET = ${dfName}")

    // // CFS Model

    // // CFS Feature Selection
    // // args(0) Dataset full location
    val fSelector = { new CFSSelector()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setOutputCol("prediction")
      .setLocallyPredictive(args(2).stripPrefix("locallyPred=").toBoolean)
      .setSearchTermination(args(3).stripPrefix("searchTermination=").toInt)
    }

    val model = 
      fSelector.fit(df.sample(withReplacement=false, fraction=sampleSize))

  }
}