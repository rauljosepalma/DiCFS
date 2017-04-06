// START SPARK CONTEXT IN SCALA REPL

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

val conf = new SparkConf().setAppName("cfs").setMaster("local[*]")
new SparkContext(conf)

import rauljosepalma.sparkmltools._

val df = DataFrameIO.readDFFromAny("/home/raul/Datasets/Large/ECBDL14/head1000-weka-discrete.arff")

import org.apache.spark.ml.feature._
val selector = new CFSSelector().setFeaturesCol("features")
val useful = selector.filterNonInformativeFeats(df)

import org.apache.spark.ml.attribute.{AttributeGroup, _}
val ag = AttributeGroup.fromStructField(useful.schema("features"))


import org.apache.spark.ml.feature._
import scala.collection.immutable.BitSet

val selector = new CfsFeatureSelector

val nFeats = 631

val correlations = new CorrelationsMatrix(631)

val adjustedPartitionSize = 5

val remainingFeats = BitSet(Range(0,nFeats).toSeq:_*)

val (partitions: Seq[BitSet], remainingFeatsPairs: Seq[(Int,Int)]) = 
selector.getPartitionsAndRemainingFeatsPairs(
  adjustedPartitionSize,
  remainingFeats,
  correlations)

// update correlations
remainingFeatsPairs.foreach( pair => correlations.data((pair._1,pair._2)) = 1.0)

val initialFeatsPairsCount = remainingFeatsPairs.size

val remainingFeats = BitSet(4, 22, 58, 59, 94, 99, 104, 105, 109, 114, 115, 117, 119, 120, 124, 126, 128, 130, 138, 146, 147, 148, 152, 153, 154, 155, 157, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 181, 188, 194, 198, 204, 208, 211, 218, 222, 225, 231, 234, 238, 240, 245, 253, 254, 256, 257, 258, 260, 262, 265, 266, 270, 278, 285, 291, 298, 305, 311, 317, 322, 325, 331, 341, 349, 351, 360, 371, 394, 411, 418, 425, 432, 433, 434, 436, 437, 438, 439, 440, 442, 445, 446, 450, 458, 462, 465, 470, 478, 485, 491, 498, 511, 512, 516, 517, 518, 519, 522, 528, 532, 534, 538, 542, 543, 558, 559, 562, 569, 576, 582, 589, 595, 602, 609, 615, 617, 619, 621)

correlations.clean(remainingFeats)

selector.getAdjustedPartitionSize(
          initialFeatsPairsCount,
          5,
          remainingFeats,
          correlations)










val llist = Seq(("bob", "2015-01-13", 4), ("alice", "2015-04-23",10))
val left = llist.toDF("name2","date","duration")
val right = Seq(("alice", 100),("bob", 23)).toDF("name1","upload")

val df = left.join(right)



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










import scala.util.control.Breaks._
def trueTypeCounter(start:Int): Int = {
  var c = 0
  for( i <- Range(start, attrsTypes.size)){
    if (attrsTypes(i)) { c += 1 } 
    if (c == 70) { return i }
  }
  return attrsTypes.size - 1
}







