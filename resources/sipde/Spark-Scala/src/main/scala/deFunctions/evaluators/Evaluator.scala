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
   *  	File:   Evaluator.scala
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
   * 		Common interface for benchmarking (i.e. fitness) functions
   * 
   *    Any new benchmarking function must extend this class to
   *    be smoothly used in the experiments taking advantage of
   *    dynamic class loading.
   *    
   *    Default implementation for configuration-related methods is 
   *    provided. These methods are only used with functions that 
   *    need to be configured and cleaned just before and after each
   *    experiment. This is the case of some biological models we 
   *    have used in our experiments through the AMIGO toolbox. 
   *    Accessing the toolbox from our Scala code was implemented
   *    by means of an SNA interface to an external C library that has to
   *    be available at runtime. 
   *     
   *		For more information on AMIGO see: 
   * 				http://gingproc.iim.csic.es/~amigo/index.html
   * 
   *		For more information on SNA see: 
   * 				https://code.google.com/archive/p/scala-native-access/
   *   
   *    Most benchmarking functions should need only to provide an 
   *    implementation for the EVALUATE abstract method.
   * 
   *  *****************************************************************
   */

package deFunctions.evaluators

// import deFunctions.Individuo
import deFunctions.DEParameters

/**
 * @author diego
 */
abstract class Evaluator extends Serializable{
  
/**
 * 
 * 	Evaluate the benchmarking function
 * 
 * @param data	Array of variable values to be evaluated
 * @param np	Dimension (number of variables) of the problem
 * @result Fitness value
 */
  def evaluate(data: Array[Double], np:Int ): Double
  
  /** 
   * 	Initial configuration of the benchmarking function is needed?
   * 
   * @return true	If an initial call to apply is needed before calling evaluate
   * @return false Otherwise
   * 
   */
  def needsConfiguration(): Boolean = {return false}
  
  /** 
   * 	Cleaning the configuration of the benchmarking function is needed?
   * 
   * @return true	If a call to clean is needed before reusing the function in a new experiment
   * @return false Otherwise
   * 
   */
  def needsCleanup(): Boolean = {return false}
  
  /** 
   * 	Initial configuration of the benchmarking function
   * 
   * @param par	Experiment parameters
   */
  def apply(par: DEParameters) = {}
  
  /**
   * Clean the configuration of the benchmarking function
   * 
   */
  def clean() = {}
}