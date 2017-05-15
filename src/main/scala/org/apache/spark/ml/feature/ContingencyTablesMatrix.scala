package org.apache.spark.ml.feature

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.math.log

class ContingencyTable extends Serializable {

  private val data =
    mutable.Map.empty[(Double,Double), Int].withDefaultValue(0)

  def apply(i: Double, j: Double) = data(i,j)
  def apply(pair: (Double,Double)): Int = data(pair)
  def update(i: Double, j: Double, value: Int) = { data((i,j)) = value }
  def update(pair: (Double,Double), value: Int) = { data(pair) = value }
  def foreach(f: (((Double,Double), Int)) => Unit): Unit = data.foreach(f)

  // Param firstFeat selects first (true) or second feature (false)
  def calcEntropyAndTotalDistinctValues(firstFeat: Boolean)
    : (Double, Int) = {
    
    // reduceByKey
    val distinctValuesCounts: Map[Double,Int] = 
      data
        .groupBy{ ((pair) => if (firstFeat) pair._1._1 else pair._1._2) }
        .map{ case (k: Double,v: mutable.Map[(Double,Double), Int]) =>
          (k, v.values.sum)
        }

    // This should be equal to the total num of instances in case of no missing
    // values
    val totalDistinctValues = distinctValuesCounts.map(_._2).sum

    // Assuming that there are no missing values
    val entropy = distinctValuesCounts
      .map{ case (_, count) => count * log(count) }
      .sum * (-1.0/totalDistinctValues) + log(totalDistinctValues)

    (entropy, totalDistinctValues)
  }

  def calcCondEntropy(
    conditioningFeatEntropy: Double, 
    totalDistinctValues: Int) : Double = {

    data    
      .map{ case (_, count) => count * log(count) }
      .sum * (-1.0/totalDistinctValues) - 
        conditioningFeatEntropy + log(totalDistinctValues)

  }
}

class ContingencyTablesMatrix(remainingFeatsPairs: Seq[(Int,Int)])
  extends Serializable {

  // keys for external map must meet: first <= last
  private val data: Map[(Int,Int), ContingencyTable] = {
    
    val cts: Seq[ContingencyTable]  =
      Seq.fill(remainingFeatsPairs.size)(new ContingencyTable)

    remainingFeatsPairs.zip(cts).toMap
  }

  def apply(i: Int, j: Int): ContingencyTable = data(i,j)
  def apply(pair: (Int,Int)): ContingencyTable = data(pair)
  def keys = data.keys
  def foreach(f: (((Int,Int), ContingencyTable)) => Unit): Unit = 
    data.foreach(f)
  
}

// An object is used for the construction outside from CfsFeatureSelector class
// to prevent the need of Serializing the whole CfsFeatureSelector class
object ContingencyTablesMatrix {

  var totalPairsEvaluated = 0

  // nFeats (must include the class) is needed only when precalcEntropies is
  // true.
  def apply(
    rdd: RDD[Row], 
    remainingFeatsPairs: Seq[(Int,Int)]): ContingencyTablesMatrix = {

    require(!remainingFeatsPairs.isEmpty, 
      "Cannot create ContingencyTablesMatrix with empty remainingFeatsPairs collection")
    // DEBUG
    println(s"EVALUATING ${remainingFeatsPairs.size} PAIRS...")
    totalPairsEvaluated += remainingFeatsPairs.size

    //DEBUG Read Matrix from disk
    // var ctmFound = false
    // var matrix: ContingencyTablesMatrix = null

    // try { 
    //   val ois = 
    //     new java.io.ObjectInputStream(new java.io.FileInputStream("./ctm"))
    //   matrix = ois.readObject.asInstanceOf[ContingencyTablesMatrix]
    //   ois.close
    //   ctmFound = true
    // } catch {
    //   case e: Exception => 
    //     println("Couldn't find a serialized ContingencyTablesMatrix, calculating it...")

    // }
    
    // if(ctmFound) ( return matrix )
    // END DEBUG


    def accumulator(matrix: ContingencyTablesMatrix, row: Row): ContingencyTablesMatrix = {

      val label = row(1).asInstanceOf[Double]
      // Label is the last feat
      val features = row(0).asInstanceOf[Vector].toArray :+ label

      // Accumulate contingency tables counts
      // Entry for (features(i),features(j)) is different that for 
      // (features(j),features(i)) (order is important)
      matrix.keys.foreach{ case (i,j) => 
        matrix(i,j)(features(i),features(j)) += 1
      }

      matrix
    }

    def merger(
      matrixA: ContingencyTablesMatrix, matrixB: ContingencyTablesMatrix):
      ContingencyTablesMatrix = {

      // matrixB.keys.foreach{ featsPair => 
      //   matrixB(featsPair).foreach{ case (featsValues, count) =>
      //     matrixA(featsIndex)(featsValues) += count
      //   }
      // }

      // Update matrixA contingency tables
      matrixB.foreach{ case(featsPair: (Int,Int), ctB: ContingencyTable) => 
        ctB.foreach{ case(valuesPairs: (Double, Double), count: Int) => 
          matrixA(featsPair)(valuesPairs) += count
        }
      }
      
      matrixA

    }

    val ctm = new ContingencyTablesMatrix(remainingFeatsPairs)
      // nFeats,
      // precalcEntropies)

    // The hard work is done in the following line
    val tmpmatrix = rdd.aggregate(ctm)(accumulator, merger)

    // TODO DELETE?
    // // If there are no remainingFeatsPairs there's no need to aggregate the dataset for doing nothing.
    // val tmpmatrix = df.rdd.aggregate(ctm)(accumulator, merger)
    //   if(!remainingFeatsPairs.isEmpty){
    //     df.rdd.aggregate(ctm)(accumulator, merger)
    //   } else {
    //     ctm
    //   }

    // DEBUG save matrix to disk
    // val oos = 
    //   new java.io.ObjectOutputStream(new java.io.FileOutputStream("./ctm"))
    // oos.writeObject(tmpmatrix)
    // oos.close
    // END DEBUG

    return tmpmatrix
  }
}