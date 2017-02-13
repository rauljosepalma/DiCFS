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
   *  	File:   LocalSolver.scala
   * 		Author: Xoan C. Pardo <xoan.pardo@udc.gal>
   *
   *   Affiliation:
   *       Grupo de Arquitectura de Computadores (GAC)
   *       Departamento de Electronica e Sistemas
   *       Facultade de Informatica
   *       Universidade da Coruña (UDC)
   *       Spain
   * 
   * 		Created on 2016-03-22
   * 
   * 		This file contains the definition of a common trait to be 
   * 		extended by custom Local Solvers.
   * 
   * 		An specialization of this trait to be used with the DE algorithm
   * 		for optimizing individuals is also included . It acts as a 
   *   wrapper for accessing an LS located in an external C library 
   *   that must be accesible at runtime.
   * 
   *  *****************************************************************
   */

package deFunctions.solvers

import java.util.concurrent.Callable
import deFunctions.DEParameters
import deFunctions.Individuo

/**
 * A common trait to be extended by custom Local Solvers
 * 
 * @author xoan
 * 
 * It extends Callable for being used with AsyncSolver
 * Subclasses MUST provide an implementation for the call method
 * 
 */
trait LocalSolver[T] extends Callable[T] {
  
  protected var initValue:T    // A default initial value MUST be provided by subclasses

  /**
   * Apply the Local Solver to optimize a given value
   * 
   * The difference between using optimize or calling directly the call method
   * is that optimize allows to pass a new initial value, but call will use the last
   * initial value provided.
   * 
   * @param i The initial value to be optimized
   *  @return the optimized value after running the Local Solver
   */
  def optimize(i:T): T = {
    initValue = i
    call()      
  }
}


/**
 *    An extension of the LocalSolver trait to be used with the DE algorithm
 *    for optimizing individuals. It acts as a wrapper for accessing an LS located in an 
 *    external C library that MUST be accesible at runtime.
 * 
 * @author xoan
 * 
 * @param params The DE and execution parameters
 */
class DELocalSolver (val params:DEParameters) extends LocalSolver[Individuo] with Serializable {  
  
  var initValue:Individuo = new Individuo(new Array[Double](params.nP))   // Default initial individual ("dummy" individual with minimal creation cost)
  private val dim:Int = params.nP  // Problem dimensionality
  //  private val solver = LocalSolverLibrary(params.solver, params.evalName)
  
  /**
   * Implementation of the overriden call method
   * 
   * @return The optimized individual after executing the Local Solver
   */
  override def call(): Individuo = { 
    // Get the LS from the external library and configure it
    println("###Starting Local Solver in thread "+ Thread.currentThread.getId() + " for: " + initValue )
    val solver = LocalSolverLibrary(params.solver, params.evalName)   // The LS name and the fitness function name to be used with the LS 
    solver.configure()

    val solverStart = System.nanoTime()

    // Prepare the input data for calling the library
    var element:Array[Double] = new Array[Double](dim+1)
    var outputElement:Array[Double] = new Array[Double](dim+1)
    for ( i <- 0 until dim){
      element(i) = initValue.values(i)
    }
    element(dim) = initValue.evaluation       

    // Call the LS in the external library
    val evals = solver.run(dim, element, outputElement)
    
    // Clean the configuration of the LS
    solver.clear()
    
    // Create a new individual with the result
    var out:Individuo = new Individuo(outputElement.slice(0, dim))
    out.evaluation = outputElement(dim)
    
    // Output times for debugging
    val solverTime = System.nanoTime()-solverStart
    println("###Local Solver time: "+ solverTime./(1000000000.0) + " s. Result: "+out.toString())

    out
   }
}



