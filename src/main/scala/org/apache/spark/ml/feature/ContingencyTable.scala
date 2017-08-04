package org.apache.spark.ml.feature

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD

import breeze.numerics.log
import breeze.linalg.DenseMatrix
import breeze.linalg.DenseVector
import breeze.linalg.sum
import breeze.linalg.*

class ContingencyTable(val matrix: DenseMatrix[Double]) extends Serializable {

  def +(that: ContingencyTable): ContingencyTable = 
    new ContingencyTable(this.matrix + that.matrix)

  def colsEntropy: Double = {
    val colsFrequencies: DenseVector[Double] = sum(matrix(::, *)).toDenseVector
    // n should be equal to nInstances in case of no missing values
    val n = sum(colsFrequencies)
    // Change zeros per ones to prevent NaN (log(1.0) == 0.0) 
    val m = colsFrequencies.map(c => if (c == 0.0) 1.0 else c)
    sum(m :* log(m)) * -1.0/n + log(n)
  }

  def rowsEntropy: Double = {
    val rowsFrequencies: DenseVector[Double] = sum(matrix(*, ::))
    // n should be equal to nInstances in case of no missing values
    val n = sum(rowsFrequencies)
    // Change zeros per ones to prevent NaN (log(1.0) == 0.0) 
    val m = rowsFrequencies.map(c => if (c == 0.0) 1.0 else c)
    sum(m :* log(m)) * -1.0/n + log(n)
  }

  // Returns conditional entropy of the rows feat conditioned on the cols
  def condEntropy(colsEntropy:Double): Double = {
    // n should be equal to nInstances in case of no missing values
    val n = sum(matrix)
    // Change zeros per ones to prevent NaN (log(1.0) == 0.0) 
    val m = matrix.map(c => if (c == 0.0) 1.0 else c)
    sum(m :* log(m)) * -1.0/n - colsEntropy + log(n)
  }

}