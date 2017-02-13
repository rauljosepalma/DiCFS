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
   *  	File:   BBOBGriewankRosenbrock.scala
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
   * 		Created on 2015-09-23
   * 
   * 		Evaluator that calls the GriewankRosenbrock benchmarking 
   *    function through the JNI fgeneric interface distributed as part 
   *    of the COCO software.
   * 		
   *   For more information on COCO see: http://coco.gforge.inria.fr 
   * 
   *  *****************************************************************
   */

package deFunctions.evaluators


import deFunctions.DEParameters
import javabbob.JNIfgeneric


/**
 * @author diego
 */
class BBOBGriewankRosenbrock extends Evaluator{
  val benchmarkID:Int = 19
  //JNIfgeneric fgeneric = new JNIfgeneric();
  val fgeneric:JNIfgeneric = new JNIfgeneric();
  var params: JNIfgeneric.Params = new JNIfgeneric.Params()
  params.algName = "Spark-DE";
  var outputPath:java.lang.String = "./BBOBOutput"
  
  var dim:Int = 0
  var libReady = false
  
  /** 
     * 	Evaluate the benchmarking function
     * 
     * @param data	Array of variable values to be evaluated
     * @param np	Dimension (number of variables) of the problem
     * @result Fitness value
     */
  def evaluate(data: Array[Double], np:Int ): Double = {
    var res = 0.0
    
    // Initialize the library
    // (would be initialized twice by the sequential version)
    if (!libReady){
      println("ReInitializing from Scala side.")
      // Create the folders for storing the experimental data
      if ( JNIfgeneric.makeBBOBdirs(outputPath, false) ) {
          println("BBOB data directories at " + outputPath
                  + " created.")
      } else {
          println("Error! BBOB data directories at " + outputPath
                  + " were NOT created, stopping.");
          sys.exit(0)
      }
      
      fgeneric.initBBOB(benchmarkID, 1, dim, outputPath, params);
      libReady=true
    }
    
    // Initialize the library
    // (would be initialized twice by the sequential version)
    if ( ! fgeneric.checkBBOB() ){
      println("ReInitializing from C side.")
      // Create the folders for storing the experimental data
      if ( JNIfgeneric.makeBBOBdirs(outputPath, false) ) {
          println("BBOB data directories at " + outputPath
              + " created.")
      } else {
          println("Error! BBOB data directories at " + outputPath
              + " were NOT created, stopping.");
          sys.exit(0)
      }
      
      fgeneric.initBBOB(benchmarkID, 1, dim, outputPath, params);
      libReady=true
    }
    
    res = fgeneric.evaluate(data);
    
    return res
  }
  
  /** 
   * 	Initial configuration of the benchmarking function is needed?
   * 
   * @return true	If an initial call to apply is needed before calling evaluate
   * @return false Otherwise
   * 
   */
  override def needsConfiguration():Boolean = {return true}
  
  /** 
   * 	Initial configuration of the BBOB benchmark library
   * 
   * @param par	Experiment parameters
   */
  override def apply(par: DEParameters) = {
    // JNIfgeneric fgeneric = new JNIfgeneric();
    
    // BBOB Mandatory initialization (already done in constructor code)
    this.dim = par.nP
    // Create the folders for storing the experimental data
    if ( JNIfgeneric.makeBBOBdirs(outputPath, false) ) {
            println("BBOB data directories at " + outputPath
                    + " created.")
        } else {
            println("Error! BBOB data directories at " + outputPath
                    + " were NOT created, stopping.");
            sys.exit(0)
        };
    
    /* Initialize the objective function in fgeneric. 
     *  funID = benchmarkID
     *  instanceID = 1 
     *  dim = par.nP
     */
    fgeneric.initBBOB(benchmarkID, 1, par.nP, outputPath, params);
    
    // Get the target function value
    par.target = fgeneric.getFtarget() 
    
  }
  
  /** 
   * 	Cleaning the configuration of the benchmarking function is needed?
   * 
   * @return true	If a call to clean is needed before reusing the function in a new experiment
   * @return false Otherwise
   * 
   */
  override def needsCleanup():Boolean = {return true}
  
  /**
   * Clean the BBOB benchmark library configuration
   * 
   */
  override def clean() = {
    // Call the BBOB closing function to wrap things up neatly
    println("Cleaning BBOBGriewankRosenbrock.")
    fgeneric.exitBBOB();
    libReady=false
  }
}