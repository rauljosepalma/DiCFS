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
   *  	File:   LocalSolverLibrary.scala
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
   *    This file contains the definition of an abstract class that
   *    implements common functionality for accessing a LS located 
   *    in an external C library. The library is expected to implement
   *    three predefined functions to configure, run and clean the LS.
   *    The library must be accesible at runtime.
   *    
   *    An specialization of this abstract class to use the NL2SOL solver
   *    in conjunction with one of three biological system models (Circadian, 
   *    NFkb and Mendes) is also provided. Both the NL2SOL and the
   *    biological models are assumed to be located in the same external 
   *    library.
   * 
   *  *****************************************************************
   */

package deFunctions.solvers


import sna.Library

/**
 * Common functionality to configure, run and clean the Local Solver
 * using SNA to interface with the external C library. 
 * 
 *		For more information on SNA see: 
 * 				https://code.google.com/archive/p/scala-native-access/ 
 * 
 * The library is supposed to be thread-safe and to allow selection
 * of the benchmarking function to be used with the LS
 * 
 * @author xoan
 * 
 */
abstract class LocalSolverLibrary {
   
  val libName:String     // Library name. MUST be provided by subclasses
  val benchmarkID:Int  // Benchmarking function id. MUST be provided by subclasses
  private val tID: Int = Thread.currentThread().getId().toInt  // Thread id
  
  // Lazy initialization of the library (this gives the time needed to load it in the workers)
  private lazy val lib: Library = Library(libName)  
  
  /**
   * Initial configuration of the Local Solver
   */
  def configure() = {
    val libSetup = lib.configure_local_solver(tID, benchmarkID)[Int]
  }
  
  /**
   * Call the Local Solver
   * 
   * @param dim The problem dimensionality
   * @param input Values of the individual to be optimized
   * @param output Values optimized by the Local Solver
   * @return Number of evalutions of the benchmarking function made by the LS
   */
  def run(dim:Int, input:Array[Double], output:Array[Double]):Long = {
    val evals = lib.run_local_solver(tID, dim, input, output)[Long]
    println("###Number of evaluations (thread "+tID+"): "+evals)
    evals
  }
  
  /**
   * Clean the Local Solver configuration
   */
  def clear() {
    val libClear = lib.clear_local_solver(tID)[Int]
  }
}

/**
 * Specialization of the LocalSolverLibrary for using our external library (SBLib)
 * 
 * @author xoan
 * 
 * @param benchmarkID Benchmarking function ID
 */
class NL2SOLLibrary(val benchmarkID:Int) extends LocalSolverLibrary {
    val libName = "SBLib"  // The external library name
}

/**
 *  Object factory (companion object)
 *  
 * @author xoan
 *  
 *  Currently it only supports the NL2SOL solver and the Circadian, Mendes and NFkb models
 *  TODO: extend the object factory to support more solvers and benchmarking functions
 */
object LocalSolverLibrary { 
  def apply(solver:String, benchmark:String):LocalSolverLibrary = solver match {
    case "NL2SOL" => benchmark match {
          case "Circadian" => new NL2SOLLibrary(0);
          case "Mendes" => new NL2SOLLibrary(1);
          case "Nfkb" => new NL2SOLLibrary(2);
    }
  }
}
