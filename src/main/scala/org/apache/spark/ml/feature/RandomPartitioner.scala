/*  Copyright (c) 2016 Diego Teijeiro, Xoán C. Pardo, Patricia González
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted for any purpose (including commercial purposes)
 * provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following acknowledgments and disclaimer.
 *     
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following acknowledgments and disclaimer
 *    in the documentation and/or other materials provided with the
 *    distribution.
 *     
 * 3. All publications mentioning features or use of this software are asked to
 *    credit the authors by citing the references provided at:
 *    https://bitbucket.org/xcpardo/sipde
 *                                   
 * ACKNOWLEDGMENTS
 * 
 * This work is the result of a collaboration between the Computer Architecture
 * Group (GAC) at Universidade da Coruña (A Coruña, Spain) and the
 * (Bio)Processing Engineering Group at IIM-CSIC (Vigo, Spain). It was produced
 * at the Universidade da Coruña (UDC) and received financial support from the
 * Spanish Ministerio de Economía y Competitividad (and the FEDER) through the
 * Project SYN-BIOFACTORY (grant number DPI2014-55276-C5-2-R). It has been also
 * supported by the Spanish Ministerio de Ciencia e Innovación (and the FEDER)
 * through the Project TIN2013-42148-P, and by the Galician Government (Xunta
 * de Galicia) under the Consolidation Program of Competitive Research Units
 * (Network Ref. R2014/041 and Project Ref. GRC2013/055) cofunded by FEDER
 * funds of the EU.
 * 
 * DISCLAIMER
 *  
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  WITH NO WARRANTY OF ANY KIND, EITHER EXPRESSED OR IMPLIED. IN NO EVENT
 *  SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DAMAGES HOWEVER
 *  CAUSED AND ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 *  ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *  
 *  */

/** 
 * *****************************************************************
 *    File: RandomPartitioner.scala
 *    Author: Xoan C. Pardo <xoan.pardo@udc.gal>
 *    Small code changes: Raúl Palma
 *
 *   Affiliation:
 *       Grupo de Arquitectura de Computadores (GAC)
 *       Departamento de Electronica e Sistemas
 *       Facultade de Informatica
 *       Universidade da Coruña (UDC)
 *       Spain
 * 
 *    Created on 2015-09-28
 * 
 *    Custom partitioner that randomly shuffles the population 
 *    guaranteeing that partition sizes will be equal
 * 
 * 
 *  *****************************************************************
 */

package org.apache.spark.ml.feature

// import org.apache.commons.math3.random.RandomDataGenerator
import scala.util.Random
import org.apache.spark.Partitioner

/**
 * Random shuffle with equal partition sizes
 * (The number of random number generation calls equals the number of elements)
 * 
 * @author xoan
 * 
 * @pre The number of elements MUST BE a multiple of the number of partitions 
 * @pre Element keys MUST BE of type Int with values in [0,number of partitions)
 */
class RandomPartitioner (
    numParts: Int = 2, 
    numElems: Int = 1000
  ) extends Partitioner {
  
  override def numPartitions = numParts    
  private val numElements = numElems
  // Number of elements per partition
  private val elementsPerPart =  numElems / numParts
  
  // Initialize an Array with the random partition each element is assigned to.
  // This must be done here in order to get all the partitions sharing the same
  // assignments
  private val partition : Array[Int] = { 
    // IDs of partitions with free places
    val partitionIDs = Array.range(0, numPartitions)
    // Free places at each partition  
    val occupation = Array.fill(numPartitions){elementsPerPart}  
    // Number of partitions with free places
    var freePartitions = numPartitions
    
    Array.fill(numElements) {  
       // Get the random ID of a partition that still has free places (0 if only one remains)
       // val index = if (freePartitions == 1) 0 else (new RandomDataGenerator()).nextInt(0, freePartitions-1)   
       val index = if (freePartitions == 1) 0 else Random.nextInt(freePartitions)   
       val partition = partitionIDs(index)
       // Update the number of free places that left in the partition 
       occupation(index) = occupation(index) - 1 
       // Exchange the partition information with the last one that still has
       // free places when no more free places left in the partition
       if (occupation(index) == 0 && freePartitions > 1) {        
         occupation(index) = occupation(freePartitions - 1)     
         partitionIDs(index) = partitionIDs(freePartitions - 1)
         // Update the number of remaining partitions with free places
         freePartitions = freePartitions - 1                                
       }          
       partition
    }
  }
    
  /**
   * Assigns a new random partition to the individual based on its key 
   * Partition sizes are guaranteed to be all the same
   * 
   * @param key The individual key
   * @return The id of the partition the individual was assigned to
   */
  override def getPartition(key: Any): Int = {
     val k = key.asInstanceOf[Int]
     
// **********  Uncomment only for debugging **********
//     println("Key: " + k + " - Index: " + partition(k)) // for local mode only
// *********************************************************
     
     partition(k)
  }
 
  /**
   * Test for equality of partitioners
   * 
   * @param other The other partitioner
   * @return true if other has the same type and same attribute values, false otherwise
   */
  override def equals(other: Any): Boolean = other match {
    case rep: RandomPartitioner =>
      (rep.numPartitions == numPartitions &&
      rep.numElements == numElements &&
      rep.partition.sameElements(partition))
    case _ =>
      false
  }
}