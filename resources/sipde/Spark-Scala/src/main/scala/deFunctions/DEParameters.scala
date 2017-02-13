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
   *  	File:   DEParameters.scala
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
   * 		Class to hold DE configuration and execution parameters
   * 
   *  *****************************************************************
   */

package deFunctions

import java.util.Properties
import java.io.FileInputStream


/**
 * @author diego
 * 

 * @param popSize Population size
 * @param nP Size of individuals (i.e. dimensionality of problem)
 * @param f Differential weight [0,2]. Several values for hetereogeneous islands
 * @param cr Crossover probability [0,1]. Several values for hetereogeneous islands
 * @param target Target fitness value (i.e. VTR: Value-to-reach stopping criteria)
 * @param maxEval Maximun number of evaluations (predefined effort stopping criteria)
 * @param maxRunTime Maximun execution time (execution time stopping criteria)
 * @param evalName Fitness function name
 * @param lowLimit 
 * @param upLimit 
 * @param lowerLimits 
 * @param upperLimits
 * @param workers Number of population partitions (i.e. islands)
 * @param localIter Number of local DE iterations during isolated island evolutions
 * @param solver Local Solver name
 * @param minDistance Minimum distance to taboo list members
 * 
 */
class DEParameters ( val popSize: Int, var nP: Int, val  f:Array[Double], val cr:Array[Double],
    var target: Double, val evalName:String, val maxEval: Int,  var lowLimit:Double, var upLimit:Double, 
    val workers:Int , val localIter:Int, var lowerLimits:Option[Array[Double]]=None, var upperLimits:Option[Array[Double]]=None, val maxRunTime:Long = 86400, val minDistance:Double=0, val solver:String="") extends Serializable{
  
  def getPopSize():Int ={
    return this.popSize
  } 
  
  override def toString():String = {
    var salida:String = "Execution parameters:\n"
    salida += " workers: "+workers+" localIterations: "+localIter+"\n"
    salida += "DE configuration:\n"
    salida += "Fitness function: "+ evalName+" Population size: "+popSize+" Dimensions: "+nP+"\n"
    salida += "F: "+f.mkString("(", ",", ")")+" CR: "+cr.mkString("(", ",", ")")+" Target value:"+target+" Max. evaluations: "+maxEval + " Max. time: "+maxRunTime+"\n"
    if (!lowerLimits.isDefined) salida += "Min. value: "+lowLimit+"\n" else salida += "Min. values: "+lowerLimits.get.mkString("[", ", ", "]")+"\n"
    if (!upperLimits.isDefined) salida += "Max. value: "+upLimit+"\n" else salida += "Max. values: "+upperLimits.get.mkString("[", ", ", "]")+"\n"
    if (solver!="") salida += "Local solver: "+solver+" Min. distance: "+minDistance+"\n" 
    return salida
  }
  
}
object DEParameters {
  
  def apply(propertiesFileName:String): DEParameters = {
    val prop = new Properties()
    prop.load(new FileInputStream(propertiesFileName))
    
    val workers = prop.getProperty("workers").toInt
    val locI = prop.getProperty("localIterations").toInt
    val maxTime = prop.getProperty("maxTime").toLong
//    val tiemposTS: Array[Double] = new Array[Double](nRuns)
    
    val evalFunc:String = prop.getProperty("evaluator")
    val nElem: Int = prop.getProperty("populationSize").toInt
    val dim = prop.getProperty("elementSize").toInt
    val f = prop.getProperty("F")
    val cr = prop.getProperty("CR")
    val target = prop.getProperty("target").toDouble
    val mEval = prop.getProperty("maxEvaluations").toInt
    val min = prop.getProperty("lowerLimit").toDouble
    val max = prop.getProperty("upperLimit").toDouble
    val minDistance = prop.getProperty("minDistance").toDouble
    val solverName:String = prop.getProperty("localSolver")
    
    val fArray:Array[Double] = f.split(",").map { x => x.toDouble }
    val crArray:Array[Double] = cr.split(",").map { x => x.toDouble }
    
    return new DEParameters(nElem, dim, fArray, crArray, target, evalFunc, mEval, min, max, workers, locI, maxRunTime=maxTime, minDistance=minDistance, solver=solverName)
    
  }
  
}