/*	Copyright (c) 2016 Diego Teijeiro, Xoán C. Pardo, Patricia González
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without modification, 
 * are permitted for any purpose (including commercial purposes) provided that the 
 * following conditions are met: 
 * 
 * 1. Redistributions of source code must retain the above copyright notice, this
 *     list of conditions and the following acknowledgments and disclaimer.
 *     
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following acknowledgments and disclaimer 
 *     in the documentation and/or other materials provided with the distribution. 
 *     
 * 3. All publications mentioning features or use of this software are asked to credit
 *     the authors by citing the references provided at: https://bitbucket.org/xcpardo/sipde
 *                                   
 * ACKNOWLEDGMENTS
 * 
 * This work is the result of a collaboration between the Computer Architecture Group (GAC)
 * at Universidade da Coruña (A Coruña, Spain) and the (Bio)Processing Engineering Group
 * at IIM-CSIC (Vigo, Spain). It was produced at the Universidade da Coruña (UDC) and 
 * received financial support from the Spanish Ministerio de Economía y Competitividad 
 * (and the FEDER) through the Project SYN-BIOFACTORY (grant number DPI2014-55276-C5-2-R). 
 * It has been also supported by the Spanish Ministerio de Ciencia e Innovación (and the FEDER)
 * through the Project TIN2013-42148-P, and by the Galician Government (Xunta de Galicia)
 * under the Consolidation Program of Competitive Research Units (Network Ref. R2014/041
 * and Project Ref. GRC2013/055) cofunded by FEDER funds of the EU.
 * 
 * DISCLAIMER
 *  
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
 *  WITH NO WARRANTY OF ANY KIND, EITHER EXPRESSED OR IMPLIED. IN NO EVENT SHALL 
 *  THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DAMAGES HOWEVER  
 *  CAUSED AND ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED  
 *  OF THE POSSIBILITY OF SUCH DAMAGE.
 *  
 *  */

/** 
   * *****************************************************************
   *  	File:   RandomPartitioner.scala
   * 		Author: Xoan C. Pardo <xoan.pardo@udc.gal>
   *
   *   Affiliation:
   *       Grupo de Arquitectura de Computadores (GAC)
   *       Departamento de Electronica e Sistemas
   *       Facultade de Informatica
   *       Universidade da Coruña (UDC)
   *       Spain
   * 
   * 		Created on 2015-08-17
   * 
   * 		Custom partitioner that randomly shuffles the population 
   * 		without a guarantee that partition sizes will be equal
   * 
   *  *****************************************************************
   */

package deFunctions.partitioners

import scala.util.Random
import org.apache.spark.Partitioner

/**
 * Random shuffle with unbalanced partition sizes
 * 
 * @author xoan
 * 
 * @param numParts The number of partitions
 */
class RandomPartitioner (numParts: Int = 2) extends Partitioner {
  override def numPartitions = numParts
 
  /**
   * Assigns a new random partition to the individual based on its key 
   * There is not guarantee about partition sizes
   * 
   * @param key The individual key
   * @return The id of the partition the individual was assigned to
   */
  override def getPartition(key: Any): Int = {
     (new Random()).nextInt(numPartitions-1)
  }
 
  /**
   * Test for equality of partitioners
   * 
   * @param other The other partitioner
   * @return true if other has the same type and number of partitions, false otherwise
   */
  override def equals(other: Any): Boolean = other match {
    case rp: RandomPartitioner =>
      rp.numPartitions == numPartitions
    case _ =>
      false
  }
}