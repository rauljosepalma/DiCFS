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
   *  	File:   Mendes.scala
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
   * 		Evaluator that calls the Mendes (3-step pathway) model 
 	 *
   *		Requirements:
   * 				+ A directory must exist containing the files
   * 				of the Mendes model used as input to the AMIGO toolbox
   * 				+ An external C library to interface with the AMIGO toolbox
   * 				has to be available to our program at runtime.
   * 
   *		For more information on AMIGO see: 
   * 				http://gingproc.iim.csic.es/~amigo/index.html
   * 
   *  *****************************************************************
   */

package deFunctions.evaluators

/**
 * @author diego
 */

import deFunctions.DEParameters
import sna.Library

class Mendes extends Evaluator{
  
  val libName = "SBLib" // name of the external C library
  var libReady = false
  val benchmarkID = 1
  
  /**
   * 
   * 	Evaluate the Mendes model
   * 
   * @param data	Array of variable values to be evaluated
   * @param np	Dimension (number of variables) of the problem
   * @result Fitness value
   */
  def evaluate(data: Array[Double], np:Int ): Double = {
    
    var sbparams: Array[Double] =Array[Double](0,0,0)     //make pointers for target, lowerLimit, upperLimit
    var intPar: Array[Int] = Array[Int](0, 0)     // Pointers for element size, second int is just to force an array of two (avoid wrong apply)
    
    // call the C library
    val lib: Library = Library(libName)
    if (!libReady){
      println("Library not ready (Scala side)")
      libReady = (lib.setup_SB_Evaluation(benchmarkID, Thread.currentThread().getId().toInt, sbparams, intPar)[Int])==1
    }
    if (lib.check_SB_initialization(Thread.currentThread().getId().toInt)[Int]==0){
        val libSetup = lib.setup_SB_Evaluation(benchmarkID, Thread.currentThread().getId().toInt, sbparams, intPar)[Int]
        println("Library not ready (C side):"+libSetup)
    }

// **********  Uncomment only for debugging **********
//    println("Evaluating..."+data.mkString("{", ", ", "}"))
// *********************************************************

   // Evaluate the model
   val res = lib.sb_evaluation(Thread.currentThread().getId().toInt, data)[Double]

// **********  Uncomment only for debugging **********
//    println("Press Enter to continue...")
//    val in = Console.readChar()
//    println("(S)Evaluated"+": "+res+": "+data.mkString("{", ", ", "}"))
// *********************************************************

    return res
  }
  
  /** 
   * 	Initial configuration of the external C library is needed?
   * 
   * @return true	If an initial call to apply is needed before calling evaluate
   * @return false Otherwise
   * 
   */
  override def needsConfiguration(): Boolean = {return true}
  
  /** 
   * 	Initial configuration of the external C library to interface with the AMIGO toolbox
   * 
   * @param par	Experiment parameters
   */
  override def apply(par: DEParameters) = {
    
    var sbparams: Array[Double] =Array[Double](0,0,0)     //make pointers for target, lowerLimit, upperLimit
    var intPar: Array[Int] = Array[Int](0, 0)     // Pointers for element size, second int is just to force an array of two (avoid wrong apply)

    //call the C library to setup system biology problems. Mendes is ID 1.
    val lib: Library = Library(libName) 
   
// **********  Uncomment only for debugging **********
//    print("Configuring Mendes: ")
// *********************************************************
    
    // Evaluate the model
    //lib.prepare_lib_data()[Int]
    val setupRes: Int = lib.setup_SB_Evaluation(benchmarkID, Thread.currentThread().getId().toInt, sbparams, intPar)[Int]
   
// **********  Uncomment only for debugging **********
//    println("Library configured ("+setupRes+") for thread "+Thread.currentThread().getId().toInt+".")
// *********************************************************
        
    // Get the limits vectors
    var lower:Array[Double] = new Array(intPar(0))
    var upper:Array[Double] = new Array(intPar(0))
    val paramsRes:Int = lib.get_limits(lower, upper, Thread.currentThread().getId().toInt)[Int]
    par.lowerLimits=Some(lower)
    par.upperLimits=Some(upper)
    
    if (setupRes>=0 )
      libReady=true
      
// **********  Uncomment only for debugging **********
//    println("Done: "+setupRes+" params: "+sbparams.mkString("|", ", ", "|"))
// *********************************************************

     // Update parameters with new values
    par.target=sbparams(0)
    par.lowLimit=sbparams(1)
    par.upLimit=sbparams(2)
    par.nP=intPar(0) 
  }
  
  /** 
   * 	Cleaning the configuration of the Mendes model is needed?
   * 
   * @return true	If a call to clean is needed before reusing the function in a new experiment
   * @return false Otherwise
   * 
   */
  override def needsCleanup():Boolean = {return true}

  /**
   * Clean the external library configuration
   * 
   */
  override def clean() = {
    
// **********  Uncomment only for debugging **********
//    println("Cleaning Mendes")
// *********************************************************
    
    val lib: Library = Library(libName)
    
    val res: Int = lib.cleanSB(Thread.currentThread().getId().toInt)[Int]
    libReady=false
    
  }
  
}