package org.apache.spark.ml.feature

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import scala.collection.mutable
import scala.collection.immutable

// remainingFeatsSize is used olny in the first time when precalcEntropies is
// true and consecuently it will be equal to nFeats
class ContingencyTablesMatrix(
  val remainingFeatsPairs: Seq[(Int,Int)],
  remainingFeatsSize: Int,
  precalcEntropies: Boolean) 
  extends Serializable {

  // keys for external map must meet: first <= last
  val tables:
    immutable.Map[(Int,Int), mutable.Map[(Double,Double), Int]] = {
    
    val featsValuesMaps: IndexedSeq[mutable.Map[(Double,Double), Int]] =
      IndexedSeq.fill(remainingFeatsPairs.size)(
        (new mutable.HashMap[(Double,Double), Int]).withDefaultValue(0))

    remainingFeatsPairs.zip(featsValuesMaps).toMap
  }

  // This is for preventing the driver to calculate the entropies from
  // the contingency tables directly a let the workers do part of the job
  // in the aggregate action.
  val featsValuesCounts: IndexedSeq[mutable.Map[Double,Int]] = {
    if(precalcEntropies)
      IndexedSeq.fill(remainingFeatsSize)(
          (new mutable.HashMap[Double,Int]).withDefaultValue(0))
    else
      IndexedSeq()
  }
  
}

// An object is used for the construction outside from CfsFeatureSelector class
// to prevent the need of Serializing the whole CfsFeatureSelector class
object ContingencyTablesMatrix {

  // It is assumed that if precalcEntropies is true then remainingFeats
  // are represented as a continous range, they also should include the class.
  //
  // It is possible that remainingFeatsPairs doesn't contain all the features
  // in remainingFeats, for example when the last partition has only one
  // element.
  def apply(
    df: DataFrame, 
    remainingFeats: Seq[Int],
    remainingFeatsPairs: Seq[(Int,Int)], 
    precalcEntropies: Boolean): ContingencyTablesMatrix = {

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

      // TODO 
      val label = row(1).asInstanceOf[Int].toDouble
      // val label = row(1).asInstanceOf[Double]
      // Select only features included in partition. Label is the last feat
      val features = row(0).asInstanceOf[Vector].toArray :+ label

      // Accumulate featsValuesCounts for entropy calculation
      if(precalcEntropies) {
        (0 until remainingFeats.size).foreach{ iFeat => 
          matrix.featsValuesCounts(iFeat)(features(iFeat)) += 1 }
      }

      // Accumulate contingency tables counts
      // Entry for (features(i),features(j)) is different that for 
      // (features(j),features(i)) (order is important)
      matrix.remainingFeatsPairs.foreach{ case (i,j) => 
        matrix.tables((i,j))(features(i),features(j)) += 1
      }

      matrix
    }

    def merger(
      matrixA: ContingencyTablesMatrix, matrixB: ContingencyTablesMatrix):
      ContingencyTablesMatrix = {

      // Update matrixA contingency tables
      matrixB.remainingFeatsPairs.foreach{ featsIndex => 
        matrixB.tables(featsIndex).foreach{ case (featsValues, count) =>
          matrixA.tables(featsIndex)(featsValues) += count
        }
      }
      
      if(precalcEntropies){
        // Update matrixA featsValuesCounts for entropy calculation
        (0 until remainingFeats.size).foreach{ iFeat =>
          matrixB.featsValuesCounts(iFeat).foreach{ case (featValue, count) =>
            matrixA.featsValuesCounts(iFeat)(featValue) += count
          }
        }
      }

      matrixA

    }

    val ctm = new ContingencyTablesMatrix(
      remainingFeatsPairs,
      remainingFeats.size,
      precalcEntropies)

    // The hard work is done in the following line
    var tmpmatrix = df.rdd.aggregate(ctm)(accumulator, merger)

    // DEBUG save matrix to disk
    // val oos = 
    //   new java.io.ObjectOutputStream(new java.io.FileOutputStream("./ctm"))
    // oos.writeObject(tmpmatrix)
    // oos.close
    // END DEBUG

    return tmpmatrix
  }
}