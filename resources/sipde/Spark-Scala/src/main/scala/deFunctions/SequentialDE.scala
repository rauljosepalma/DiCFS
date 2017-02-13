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
   *  	File:   SequentialDE.scala
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
   * 		Implementation of the sequential version of the DE algorithm
   * 
   *  *****************************************************************
   */

package deFunctions

import org.apache.commons.math3.random.RandomDataGenerator
import deFunctions.evaluators.Evaluator
import deFunctions.mutators.Mutator
import org.apache.spark.Partitioner
import scala.collection.mutable.ListBuffer
import deFunctions.mutators.DEMutatorParams

/**
 * @author diego
 * 
 * @param params Parameters of the experiment
 * @param eval Evaluation strategy
 * @param mutator Mutation strategy
 * @param partitioner Migration strategy (random shuffle) -> not used in the sequential version
 * 
 */
class SequentialDE(val params:DEParameters, val eval:Evaluator, val mutator:Mutator, val partitioner:Partitioner) extends Optimizer {
  
//  super(params, eval, mutator, partitioner)

  /**
   *  * @author diego
   * 
   *  The method that executes the algorithm
   */
  def run():Individuo = {
    var i:Int = 0
    var nEval:Int = 0  // number of evaluations
    var gen:Int = 0  // generation counter
    var lastTime: Long = System.nanoTime()
    val runTime_I: Long = System.nanoTime()
    var runTime:Long = 0
    
    //Initialize random population
    var poblacion: Array[Individuo] = new Array[Individuo](params.getPopSize())   
    for (i <- 0 until params.getPopSize()){
//      if (i==0)
//        poblacion(i) = new Individuo(parameters, el1)
//      else if (i==1)
//        poblacion(i) = new Individuo(parameters, el2)
//      else
      poblacion(i) = new Individuo(params)
      poblacion(i).evaluate(eval.evaluate) // evaluate the individual using the fitness function of the evaluation strategy
      nEval+=1
    }
    
    // Initialize aux vars
    var rand: RandomDataGenerator = new RandomDataGenerator()
    var parada = false    // stopping condition
    var bestInd:Int = -1  // best individual
    var bestValue: Double = Double.MaxValue  // best fitness value
    
    var time1:Long = 0
    var time2:Long = 0
    
    val evalInfo = params.maxEval/100  // used to decide when to log evolution info
    
    // main loop 
    while (!parada){
      gen+=1  
      if (nEval%evalInfo < 1000){         //<1000 in case it's not exact, for whatever reason
        val newTime = System.nanoTime()
        println("Number of evaluations: "+nEval+
            " Time since last check: "+(newTime-lastTime)./(1000000000.0) + " s. bV: "+bestValue)
        lastTime = newTime
      }
        
      // for each individual calculate a new solution
      for (i <- 0 until params.popSize){
        val time1_I = System.nanoTime()
        // pick five different random individuals from population 
        // Note: this is to support both DE/Rand/1 and DE/Rand/2 mutation strategies
        var indA = rand.nextInt(0, params.popSize-1)
        while (indA == i)
          indA = rand.nextInt(0, params.popSize-1)
        var indB = rand.nextInt(0, params.popSize-1)
        while (indB==indA || indB == i)
          indB = rand.nextInt(0, params.popSize-1)
        var indC = rand.nextInt(0, params.popSize-1)
        while (indC==indB || indC == indA || indC == i)
          indC = rand.nextInt(0, params.popSize-1)
          var indD = rand.nextInt(0, params.popSize-1)
        while (indD==indC || indD==indB || indD == indA || indD == i)
          indD = rand.nextInt(0, params.popSize-1)
        var indE = rand.nextInt(0, params.popSize-1)
        while (indE==indD || indE==indC || indE==indB || indE == indA || indE == i)
          indE = rand.nextInt(0, params.popSize-1)
        val fathers = new ListBuffer[Individuo]()
        fathers.+=(poblacion(i))
        fathers.+=(poblacion(indA))
        fathers.+=(poblacion(indB))
        fathers.+=(poblacion(indC))
        fathers.+=(poblacion(indD))
        fathers.+=(poblacion(indE))
          
        val a = poblacion(indA)
        val b = poblacion(indB)
        val c = poblacion(indC)
        val d = poblacion(indD)
        val e = poblacion(indE)
        
        // Apply mutation strategy to generate a new individual (candidate solution)
//        var child: Individuo = mutator.mutate(params, poblacion(i), a, b, c)
        var child:Individuo = mutator.mutate(new DEMutatorParams(params.nP, params.f(0), params.cr(0)), fathers.toList)
        
        time1 += System.nanoTime() - time1_I
        
        val time2_I = System.nanoTime()
        // Evaluate the new candidate solution and replace the original if it is better
        child.evaluate(eval.evaluate)
        nEval+=1
        if (poblacion(i).evaluation>child.evaluation){
          poblacion(i) = child
        }
        time2 += System.nanoTime() - time2_I
      }

      // Select the best solution found until now
      for (i <- 0 until params.popSize){
        if (poblacion(i).evaluation<bestValue){
          bestValue = poblacion(i).evaluation
              bestInd = i
        }
      }
      
      // Check stopping criterion: best solution reached target value?
      if (bestValue<=params.target){
        println("Found a solution: bV:"+bestValue+" Target value: "+params.target)
        parada = true
      }

      // Check stopping criterion: number of evaluations reached maximum allowed?
      if (nEval>params.maxEval){
        parada=true
        println("Stopping execution because the max. number of evaluations was reached.")
      }

      // Check stopping criterion: execution time reached maximum allowed?
      runTime = System.nanoTime() - runTime_I  
      if (runTime > params.maxRunTime*1000000000.0){
        parada=true
        println("Stopping execution because the max. execution time was reached.")
      }      
    }
    
    // log results
    if (bestValue > params.target){
      println("No valid solution was found. Best: "+bestValue)
      //return null
    }
    println("Number of fitness evaluations: "+nEval)
    println("Time in stage 1: " + time1./(1000000000.0) +" s. Time in stage 2: " + time2./(1000000000.0) +" s.")
    println("Number of generations: "+gen)
    
    return poblacion(bestInd)
    
  }
}