package org.apache.spark.ml.feature

import scala.math.round
import scala.collection.mutable

class CorrelationsMatrix(correlator: Correlator) {

  private val corrs = mutable.Map.empty[(Int,Int), Double]

  // iPartners collection must be asc sorted
  def precalcCorrs(iFixedFeat: Int, iPartners: Seq[Int]): Unit = {

    // In the case a of feature that was evaluated and expanded and then not
    // added (causing a fail), it is possible that some of the iPartners sent
    // have already been evaluated. However, test showed that filtering the
    // list takes more time than processing it as is.
    // val filteredIPartners = iPartners.filter{ iPartner => 
    //   val key: (Int, Int) = 
    //     if (iPartner < iFixedFeat) (iPartner,iFixedFeat) 
    //     else (iFixedFeat,iPartner)
    //   !corrs.contains(key) 
    // }

    if(!iPartners.isEmpty){

      // The hard work line!
      val newCorrs: Seq[Double] = correlator.correlate(iFixedFeat, iPartners)
        // correlator.correlate(iFixedFeat, filteredPartners)
        
      // Update corrs
      iPartners.zip(newCorrs).foreach{ case (iPartner, corr) => 
        this(iPartner, iFixedFeat) = corr
      }

      // DEBUG print corrs with class
      // println("CORRS WITH CLASS=")
      // println(newCorrs.mkString(","))
      // System.exit(0)
    }
  }

  // TODO test if filtering helps here or using precalcCorrs is better

  // iPartners collection must be asc sorted
  // This method is useful when there is a possibility that all the pairs have
  // already been evaluated, as when adding predictive feats.
  def precalcNonExistentCorrs(iFixedFeat: Int, iPartners: Seq[Int]): Unit = {

    val filteredIPartners = iPartners.filter{ iPartner => 
      val key: (Int, Int) = 
        if (iPartner < iFixedFeat) (iPartner,iFixedFeat) 
        else (iFixedFeat,iPartner)
      !corrs.contains(key) 
    }

    if(!filteredIPartners.isEmpty){

      // The hard work line!
      val newCorrs: Seq[Double] = 
        correlator.correlate(iFixedFeat, filteredIPartners)
        // correlator.correlate(iFixedFeat, filteredPartners)
        
      // Update corrs
      filteredIPartners.zip(newCorrs).foreach{ case (iPartner, corr) => 
        this(iPartner, iFixedFeat) = corr
      }

      // DEBUG print corrs with class
      // println("CORRS WITH CLASS=")
      // println(newCorrs.mkString(","))
      // System.exit(0)
    }
  }

  def apply(i: Int, j: Int): Double = {
    val key: (Int, Int) = if (i < j) (i,j) else (j,i)
    corrs(key)
  }

  def apply(pair: (Int,Int)): Double = {
    val key: (Int, Int) = if (pair._1 < pair._2) pair else pair.swap
    corrs(key)
  }

  def update(i: Int, j: Int, value: Double): Unit = {
    val key: (Int, Int) = if (i < j) (i,j) else (j,i)
    corrs(key) = value
  }

  
  def size: Int = corrs.size

  override def toString(): String =  {
    this.corrs.keys.toSeq.sorted
      .map{ pair => s"$pair = ${corrs(pair)}" }.mkString("\n")
  }

}