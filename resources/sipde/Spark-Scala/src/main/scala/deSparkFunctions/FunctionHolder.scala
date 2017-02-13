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
   *  	File:   FunctionHolder.scala
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
   * 		Functions to be passed to Spark workers
   * 
   *   Note: most of these functions are deprecated. Currently we
   *   are only using a couple of them. 
   * 
   *  *****************************************************************
   */

package deSparkFunctions

import deFunctions.Individuo
import deFunctions.evaluators.Evaluator
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.commons.math3.random.RandomDataGenerator
import deFunctions._
import deFunctions.mutators.Mutator
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import deFunctions.mutators.DEMutatorParams


/**
 * A class to hold the functions to be passed to Spark workers
 * 
 * @author diego
 * 
 * @param eval The evaluation strategy (i.e. fitness function)
 * @param mut The mutation strategy
 * @param params The experiment parameters
 */
class FunctionHolder (val eval:Evaluator, val mut:Mutator, val params:DEParameters) extends Serializable{

// **********  Uncomment only for debugging **********
//    println("eval: "+eval)
//    println("mut: "+mut)
//    println("params: "+params)
// *********************************************************
  
//  var eval:Evaluator = null
//  var mut:Mutator = null
//  var params:DEParameters = null

//  def apply( evaluator:Evaluator, mutator:Mutator, parameters:DEParameters) = {
//    eval = evaluator
//    mut = mutator
//    params = parameters
//    
//    println("eval: "+eval)
//    println("mut: "+mut)
//    println("params: "+params)
//  }
    
  //var groups: Array[Int] = new Array[Int](params.popSize)
  
  
  /**
   * An island local evolution
   *  
   * @param lst An iterator to the initial island population
   * @return An iterator to the new island population
   * @deprecated Used in a early island-based DE implementation
   */
  def partitionMap( lst: Iterator[(Int, Individuo)]): Iterator[(Int, Individuo)] = {
    //variables
    var parada = false
    var iterLoc = 0
    val rand:RandomDataGenerator = new RandomDataGenerator()

// **********  Uncomment only for debugging **********
//    System.load("/root/SparkFolder/libclib.so")
//    val env = System.getenv
//    val properties = System.getProperties
//    println("env:")
//    for ((k,v) <- env) println(s"key: $k, value: $v")
//    println("Properties:")
//    for ((k,v) <- properties) println(s"key: $k, value: $v")
// *********************************************************

    var oldPop = lst.toList
    var newPop: List[(Int, Individuo)] = List()
    val localIter_I = System.nanoTime()
    
// **********  Uncomment only for debugging **********
    //print all index
//    println("***************************")
//    var ind = "Indices: "
//    for (i <- 0 until oldPop.size){
//      ind += oldPop(i)._1+", "
//    }
//    println(ind)
// *********************************************************    
    
    // Loop localIter iterations
    while (!parada){
      iterLoc += 1
      
      // New population
      newPop = List()
      
      // for each individual calculate a new solution
      for (i <- 0 until oldPop.size){
        var father:Individuo = oldPop(i)._2
        // pick three different random individuals from population 
        var indA = rand.nextInt(0, oldPop.size-1)
        while (indA == i)
          indA = rand.nextInt(0, oldPop.size-1)
        var indB = rand.nextInt(0, oldPop.size-1)
        while (indB==indA || indB == i)
          indB = rand.nextInt(0, oldPop.size-1)
        var indC = rand.nextInt(0, oldPop.size-1)
        while (indC==indB || indC == indA || indC == i)
          indC = rand.nextInt(0, oldPop.size-1)
          
//        var a :Individuo = oldPop(indA)._2
//        var b :Individuo = oldPop(indB)._2
//        var c :Individuo = oldPop(indC)._2
        
        val fathers = new ListBuffer[Individuo]()
        fathers.+=(oldPop(i)._2)
        fathers.+=(oldPop(indA)._2)
        fathers.+=(oldPop(indB)._2)
        fathers.+=(oldPop(indC)._2)
        
// **********  Uncomment only for debugging **********
//        println("Checking for nulls")
//        //check for nulls
//        if (father==null){
//          println("Father es null")
//        }
//        if (a==null){
//          println("A es null")
//        }
//        if (b==null){
//          println("B es null")
//        }
//        if (c==null){
//          println("C es null")
//        }
//        if (params==null){
//          println("params es null")
//        }
// *********************************************************
        
        // Apply mutation strategy to generate a new individual (candidate solution)
        var child:Individuo = mut.mutate(new DEMutatorParams(params.nP, params.f(0), params.cr(0)), fathers.toList)
        // Evaluate the new candidate solution and replace the original if it is better
        child.evaluate(eval.evaluate)
        if (child.evaluation<father.evaluation)
          newPop = newPop.:+(oldPop(i)._1, child)
        else
          newPop = newPop.:+(oldPop(i)._1, father)
      // Check stopping criterion: best solution reached target value?
        if (child.evaluation<=params.target){
          parada = true
          // calculate iteration time
          val localIter_F = System.nanoTime()
//          timeAccum.add(localIter_F-localIter_I)
          // store number of evaluations
//          evalsAccum.add(iterLoc)
        }
      }
      
      
      // Check stopping criterion: number of iterations reached maximum allowed?
      if (!parada)
        parada = iterLoc>=params.localIter
      
      // update the population
      oldPop = newPop
    }
    
    return newPop.iterator
  }
  
  /**
   *  pick three different random indexes from population (distributed version)
   *  
   * @param element The individual to be mutated
   * @return A tuple containing the indivual index and the three randomly generated different indexes 
   * @deprecated Used in the early master-slave DE implementation
   */
  def calculateIndexMap( element: (Int, Individuo) ): (Int, (Int, Int, Int))={
    
    var rand: RandomDataGenerator = new RandomDataGenerator()
    var indA = rand.nextInt(0, params.popSize-1)
        while (indA == element._1)
          indA = rand.nextInt(0, params.popSize-1)
        var indB = rand.nextInt(0, params.popSize-1)
        while (indB==indA || indB == element._1)
          indB = rand.nextInt(0, params.popSize-1)
        var indC = rand.nextInt(0, params.popSize-1)
        while (indC==indB || indC == indA || indC == element._1)
          indC = rand.nextInt(0, params.popSize-1)
    
    return (element._1, (indA, indB, indC))
  }
  
  /**
   *  Generate a new candidate solution from three random individuals (distributed version)
   *  
   * @param element A tuple containing the indivual to be mutated and three random individuals to be mutated from
   * @return The new candidate solution
   * @deprecated Used in the early master-slave DE implementation
   */
  def mutateWithFathers(element:(Int, (Individuo, Individuo, Individuo, Individuo))  ):(Int, Individuo) = {
    
    val fathers = new ListBuffer[Individuo]()
    fathers.+=(element._2._1)
    fathers.+=(element._2._2)
    fathers.+=(element._2._3)
    fathers.+=(element._2._4)
    
    // Apply mutation strategy to generate a new individual (candidate solution)
    var child = mut.mutate(new DEMutatorParams(params.nP, params.f(0), params.cr(0)), fathers.toList)
    // Evaluate the new candidate solution and replace the original if it is better
    child.evaluate(eval.evaluate)
    if (child.evaluation< element._2._1.evaluation){
      return (element._1, child)
    }
    return (element._1, element._2._1)
  }
  
  
//  def mutateList (element: (Int, Iterable[Individuo])) = {
//    
//    var elList = element._2.toList
//    var iterator = elList.iterator
//    var child:Individuo = null   
//    
//    if (elList.size==4){}
//      child = mut.mutate(elList.head, elList(1), elList(2), elList(3), params)
//      
//    }
//    
//    
//  }
//    def emptyArray() = {
//    
//    groups = new Array[Int](params.popSize)
//    for (i <- 0 until params.popSize)
//      groups(i)=0
//  }
  
  
  /**
   * Check stopping criterion: any solution reached target value? 
   *  
   * @param element A candidate solution 
   * @return true if the solution reached the target value, false otherwise
   */  
  def checkStop( element: (Int, Individuo) ): Boolean = {
    
    return element._2.evaluation<=params.target
  }
  
  /**
   * Gather best individual
   *  
   * @params a,b Candidate solutions 
   * @return The best of a and b
   */
  def finalReduction(a: (Int, Individuo), b:(Int, Individuo) ): (Int, Individuo) = {
    if (a._2.evaluation<b._2.evaluation)
      return a
    else 
      return b
    
  }

  /**
   *  Evaluate a candidate solution
   *  
   * @param pair A candidate solution
   * @deprecated
   */  
  def evalElement (pair:(Int, Individuo))={
    pair._2.evaluate(eval.evaluate)
  }
}