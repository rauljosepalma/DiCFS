package org.apache.spark.ml.feature

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import scala.collection.mutable
import scala.collection.immutable

// nFeats parameter should include the class
class ContingencyTablesMatrix(val nFeats: Int) extends Serializable {

  val featsIndexes: immutable.Vector[(Int,Int)] = 
    (0 until nFeats).combinations(2).toVector.map{ 
      case immutable.Vector(i,j) => (i,j) }

  // keys for external map must meet: first <= last
  val tables:
    immutable.Map[(Int,Int), mutable.Map[(Int,Int), Int]] = {
    
    val featsValuesMaps: immutable.Vector[mutable.Map[(Int,Int), Int]] =
      immutable.Vector.fill(featsIndexes.size)((new mutable.HashMap[(Int,Int), Int]).withDefaultValue(0))

    featsIndexes.zip(featsValuesMaps).toMap
  }

  // This is for preventing the driver to calculate the entropies from
  // the contingency tables directly a let the workers do part of the job
  // in the aggregate action.
  val featsValuesCounts: immutable.Vector[mutable.Map[Int,Int]] =
    immutable.Vector
      .fill(nFeats)((new mutable.HashMap[Int,Int]).withDefaultValue(0))
  
}

// An object is used for the construction outside from CfsFeatureSelector class
// to prevent the need of Serializing the whole class
object ContingencyTablesMatrix {

  def apply(df: DataFrame, nFeats: Int): ContingencyTablesMatrix = {

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

    def accumulator (matrix: ContingencyTablesMatrix, row: Row):
      ContingencyTablesMatrix = {

      val features: IndexedSeq[Int] = 
        row(0).asInstanceOf[Vector].toArray.map(_.toInt) :+ 
        row(1).asInstanceOf[Double].toInt

      // Accumulate featsValuesCounts for entropy calculation
      (0 until matrix.nFeats).foreach{ iFeat => 
        matrix.featsValuesCounts(iFeat)(features(iFeat)) += 1 }

      // Accumulate contingency tables counts
      // Entry for (features(i),features(j)) is different that for 
      // (features(j),features(i)) (order is important)
      matrix.featsIndexes.foreach{ case (i,j) => 
        matrix.tables((i,j))(features(i),features(j)) += 1
      }

      matrix
    }

    def merger (
      matrixA: ContingencyTablesMatrix, matrixB: ContingencyTablesMatrix):
      ContingencyTablesMatrix = {

      // Update matrixA contingency tables
      matrixB.featsIndexes.foreach{ featsIndex => 
        matrixB.tables(featsIndex).foreach{ case (featsValues, count) =>
          matrixA.tables(featsIndex)(featsValues) += count
        }
      }
      
      // Update matrixA featsValuesCounts for entropy calculation
      (0 until matrixB.nFeats).foreach{ iFeat =>
        matrixB.featsValuesCounts(iFeat).foreach{ case (featValue, count) =>
          matrixA.featsValuesCounts(iFeat)(featValue) += count
        }
      }

      matrixA

    }

    // The hard work is done in the following line
    var tmpmatrix = df.rdd.aggregate(
      new ContingencyTablesMatrix(nFeats))(accumulator, merger)

    // DEBUG save matrix to disk
    // val oos = 
    //   new java.io.ObjectOutputStream(new java.io.FileOutputStream("./ctm"))
    // oos.writeObject(tmpmatrix)
    // oos.close
    // END DEBUG

    return tmpmatrix
  }
}