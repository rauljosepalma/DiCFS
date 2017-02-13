package org.apache.spark.ml.feature

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint


import scala.collection.mutable
import scala.collection.immutable

// nFeats parameter should include the class
class ContingencyTablesMatrix(val nFeats: Int) extends java.io.Serializable {

  val featsIndexes: Vector[(Int,Int)] = 
    (0 until nFeats).combinations(2).toVector.map{ case Vector(i,j) => (i,j) }

  // keys for both external and internal map must meet: first <= last
  val tables:
    immutable.Map[(Int,Int), mutable.Map[(Int,Int), Int]] = {
    
    val featsValuesMaps: Vector[mutable.Map[(Int,Int), Int]] =
      Vector.fill(featsIndexes.size)((new mutable.HashMap[(Int,Int), Int]).withDefaultValue(0))

    featsIndexes.zip(featsValuesMaps).toMap
  }

  // This is for preventing the driver to calculate the entropies from
  // the contingency tables directly a let the workers do part of the job
  // in the aggregate action.
  val featsValuesCounts: Vector[mutable.Map[Int,Int]] =
    Vector.fill(nFeats)((new mutable.HashMap[Int,Int]).withDefaultValue(0))
  
}

// An object is use for the construction no inside any other class to prevet 
// the need of Serializing the whole class
object ContingencyTablesMatrix {

  def apply(data: RDD[LabeledPoint], nFeats: Int): ContingencyTablesMatrix = {

    def accumulator (
      matrix: ContingencyTablesMatrix, lp: LabeledPoint): 
      ContingencyTablesMatrix = {

      val features: IndexedSeq[Int] = 
        lp.features.toArray.map(_.toInt) :+ lp.label.toInt

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
    data
      .aggregate(new ContingencyTablesMatrix(nFeats))(accumulator, merger)
  }
}