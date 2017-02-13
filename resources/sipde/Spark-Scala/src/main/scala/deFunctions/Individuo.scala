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
   *  	File:   Individuo.scala
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
   * 		Class representing an individual (a candidate solution for the problem)
   * 
   *  *****************************************************************
   */

package deFunctions

import org.apache.commons.math3.random.RandomDataGenerator


/**
 * @author diego
 * 
 * @param values Individual values for the problem variables
 */
@SerialVersionUID(100L)
class Individuo (var values: Array[Double]) extends Serializable{
  
  var evaluation: Double = -1  // fitness value

  /**
   * Generates and evaluates a new individual with random data
   * 
   * @param params Parameters of the experiment
   * @param evalfunc Fitness function
   */
  def this(params: DEParameters, evalFunc:(Array[Double], Int ) => Double) = {
    this(new Array[Double](params.nP));

// **********  Uncomment only for debugging **********
//    var orig:String = "Original: "
//    for (i <- 0 until np){
//      orig+=this.values(i)+", "
//    }
//    
//    var st:String= "first constructor: "
// *********************************************************
    
    val rand: RandomDataGenerator = new RandomDataGenerator()
    
    //generation with only one lower and upper limit
    if (!params.lowerLimits.isDefined){
      val slope:Double = (params.upLimit - params.lowLimit)

// **********  Uncomment only for debugging **********
//      st+="One Limit: "
// *********************************************************

      for (i <- 0 until params.nP){
        val rnd = rand.nextUniform(0,1)
        this.values(i) = params.lowLimit + slope * rnd

// **********  Uncomment only for debugging **********
//        st+=this.values(i)+", "
// *********************************************************

//        if (rnd<=0.5)
//          values(i) = rnd*2*params.lowLimit
//        else
//          values(i) = (rnd-0.5)*2*params.upLimit
      }
    }

    //generation with per-component lower and upper limit
    if (params.lowerLimits.isDefined){
      
// **********  Uncomment only for debugging **********
//      st+="multiple Limits: "
// *********************************************************

      for (i <- 0 until params.nP){
        val rnd = rand.nextUniform(0,1)
        this.values(i) = params.lowLimit + (params.upperLimits.get.apply(i) - params.lowerLimits.get.apply(i)) * rnd
        
// **********  Uncomment only for debugging **********
//        st+=this.values(i)+", "
// *********************************************************

//        if (rnd<=0.5)
//          values(i) = rnd*2*params.lowLimit
//        else
//          values(i) = (rnd-0.5)*2*params.upLimit
      }
    }

// **********  Uncomment only for debugging **********
//    println(st)
// *********************************************************

    evaluation = evalFunc(this.values, this.values.size)

// **********  Uncomment only for debugging **********
//    println(orig)
// *********************************************************
  }
  
  /**
   * Generates a new individual with random data
   *   (without evaluating it)
   * 
   * @param params Parameters of the experiment
   */
  def this(params: DEParameters)={
    this(new Array[Double](params.nP));
    val rand: RandomDataGenerator = new RandomDataGenerator()

// **********  Uncomment only for debugging **********
//    val slope:Double = (params.upLimit - params.lowLimit)
//    var i:Int = 0
    
//    var st:String= "Second constructor: "
// *********************************************************

    //generation with only one lower and upper limit
    if (!params.lowerLimits.isDefined){

// **********  Uncomment only for debugging **********
//      st+="One Limit: "
// *********************************************************

      val slope:Double = (params.upLimit - params.lowLimit)
      for (i <- 0 until params.nP){
        val rnd = rand.nextUniform(0,1)
        this.values(i) = params.lowLimit + slope * rnd
        
// **********  Uncomment only for debugging **********
//        st+=this.values(i)+", "
// *********************************************************
        
//        if (rnd<=0.5)
//          values(i) = rnd*2*params.lowLimit
//        else
//          values(i) = (rnd-0.5)*2*params.upLimit
      }
    }
    
    //generation with per-component lower and upper limit
    if (params.lowerLimits.isDefined){

// **********  Uncomment only for debugging **********
//      st+="multiple Limits: "
// *********************************************************
      
      for (i <- 0 until params.nP){
        val rnd = rand.nextUniform(0,1)
        this.values(i) = params.lowLimit + (params.upperLimits.get.apply(i) - params.lowerLimits.get.apply(i)) * rnd
        
// **********  Uncomment only for debugging **********
//        st+=this.values(i)+", "
// *********************************************************
        
//        if (rnd<=0.5)
//          values(i) = rnd*2*params.lowLimit
//        else
//          values(i) = (rnd-0.5)*2*params.upLimit
      }
    }
    
// **********  Uncomment only for debugging **********
//    println(st)
// *********************************************************
    
  }

  /**
   * Evaluates the individual
   * 
   * @param evalfunc Fitness function
   */
  def evaluate(evalFunc:(Array[Double], Int ) => Double ) = {
// **********  Uncomment only for debugging **********
//    println("evaluation: "+this.values.mkString(", "))
// *********************************************************

    evaluation = evalFunc(this.values, this.values.size)
    
// **********  Uncomment only for debugging **********
//    println("Evaluation: "+evaluation)
// *********************************************************
  }
  
  override def toString():String = {
    var st:String = "{ "+this.evaluation+" <-("
    for (i <- 0 until this.values.size-1){
      st+=this.values(i)+", "
    }
    st+=this.values(this.values.size-1)+")}"
    return st
  } 
  
  /**
   * Compare two individuals by fitness
   * 
   * @params a,b Individuals to compare
   */
  def compare(a:Individuo, b:Individuo) = a.evaluation compare b.evaluation
}

/**
 * An ordering to compare two individuals by fitness
 * 
 * @author xoan
 * 
 */
@SerialVersionUID(100L)
object IndividuoOrdering extends Ordering[Individuo] with Serializable {
  def compare(a:Individuo, b:Individuo) = a.evaluation compare b.evaluation
}