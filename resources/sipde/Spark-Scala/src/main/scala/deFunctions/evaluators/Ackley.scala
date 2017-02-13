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
   *  	File:   Ackley.scala
   * 		Author: Diego Teijeiro <diego.teijeiro@udc.es>
   * 		Under direction and supervision by: 
   * 							Xoan C. Pardo <xoan.pardo@udc.gal> and
   * 						  Patricia Gonzalez <patricia.gonzalez@udc.es>
   *
   *   Affiliation:
   *       Grupo de Arquitectura de Computadores (GAC)
   *       Departamento de Electronica e Sistemas
   *       Facultade de Informatica
   *       Universidade da Coruña (UDC)
   *       Spain
   * 
   * 		Created on 2015-08-10
   * 
   * 		Our own implementation of the Ackely benchmarking function
   * 
   *  *****************************************************************
   */

package deFunctions.evaluators

import scala.math

/**
 * @author diego
 */
class Ackley extends Evaluator{
  
  /** 
     * 	Evaluate the benchmarking function
     * 
     * @param data	Array of variable values to be evaluated
     * @param np	Dimension (number of variables) of the problem
     * @result Fitness value
     */
  def evaluate(data: Array[Double], np:Int ): Double = {
    var result = 0.0
    
    //loop 0<i<np [0,np-1]
    var sum1 = 0.0
    for (i <- 0 until np){
      sum1+=data(i)*data(i)
    }
    //loop 0<i<np [0,np-1]
    var sum2 = 0.0
    for (i <- 0 until np){
      sum2+=scala.math.cos(2*scala.math.Pi*data(i))
    }
    
    result = -20*scala.math.exp(-0.2*scala.math.sqrt(1/np*sum1))
    result -= scala.math.exp(1/np*sum2)
    result += 20 + scala.math.E
    
    return result
  }
  
}