


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







