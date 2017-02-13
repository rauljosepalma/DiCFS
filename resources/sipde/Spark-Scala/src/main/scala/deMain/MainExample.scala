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
   *  	File:   MainExample.scala
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
   * 		Main program used to configure and run the experiments
   * 
   *  *****************************************************************
   */


package deMain

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.{Level, Logger}
import scala.App
import deFunctions.DEParameters
import deFunctions.evaluators._
import deFunctions.mutators._
import deFunctions.Individuo
import java.util.Properties
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import org.apache.spark.Partitioner
import scala.collection.JavaConversions._
import deFunctions.partitioners.EnhancedRandomExactPartitioner
import deFunctions.Individuo
import java.util.concurrent.CountDownLatch
import java.util.concurrent.FutureTask
import java.util.concurrent.{Executors, ExecutorService}
import deFunctions.solvers._
import deFunctions.Optimizer
import deFunctions.SequentialDE
import deFunctions.SparkIslandsDE
import sna.Library
//import deFunctions.solvers.LocalSolver



object MainExample extends App{

  
  override def main(args:Array[String]) {

    val jobName = "MainExample"
    val level = Level.WARN
    var config:String = ""
    var out:String = ""
    
    
    // Set input configuration file and (optionally) output file
    if (args.size==1) {
      //config file
      config = args(0)
    } else if (args.size==2) {
      config = args(0)
      out = args(1)       //output file
    } else {
      println("Insufficient arguments.")
      println("Usage: AppName <configFile> [<OutputFile>]")
      return
    }
    
    // Load input configuration file and initialize experiment parameters
    print("Loading experiment parameters...")
     val prop = new Properties()
    prop.load(new FileInputStream(config))
    val localRun = prop.getProperty("localRun").toBoolean
    val secFirst=prop.getProperty("SecuentialFirst").toBoolean
    val parRuns = prop.getProperty("parRuns").toInt
    val secRuns = prop.getProperty("secRuns").toInt
    val workers = prop.getProperty("workers").toInt
    val target = prop.getProperty("target").toDouble
    val tiemposSec: Array[Double] = new Array[Double](secRuns)
    val tiemposPar: Array[Double] = new Array[Double](parRuns)
    
//    val prop = new Properties()
//    prop.load(new FileInputStream(config))
//    val workers = prop.getProperty("workers").toInt
//    val localRun = prop.getProperty("localRun").toBoolean
//    val locI = prop.getProperty("localIterations").toInt
//    val secFirst=prop.getProperty("SecuentialFirst").toBoolean
//    val parRuns = prop.getProperty("parRuns").toInt
//    val secRuns = prop.getProperty("secRuns").toInt
//    val tiemposSec: Array[Double] = new Array[Double](secRuns)
//    val tiemposPar: Array[Double] = new Array[Double](parRuns)
//    val maxTime = prop.getProperty("maxTime").toLong
//    val tiemposTS: Array[Double] = new Array[Double](nRuns)
    
//    val evalFunc:String = prop.getProperty("evaluator")
//    val nElem: Int = prop.getProperty("populationSize").toInt
//    val dim = prop.getProperty("elementSize").toInt
//    val fLower = prop.getProperty("FLower").toDouble
//    val fUpper = prop.getProperty("FUpper").toDouble
//    val crLower = prop.getProperty("CRLower").toDouble
//    val crUpper = prop.getProperty("CRUpper").toDouble
//    val target = prop.getProperty("target").toDouble
//    val mEval = prop.getProperty("maxEvaluations").toInt
//    val min = prop.getProperty("lowerLimit").toDouble
//    val max = prop.getProperty("upperLimit").toDouble
//    val minDistance = prop.getProperty("minDistance").toDouble
//    val solverName:String = prop.getProperty("localSolver")
    
    
    // Redirect output to file (if provided) 
    if (out!=""){
      println("Redirecting output to: "+out)
      //val fosOUT = new FileOutputStream(out)
      Console.setOut(new FileOutputStream(out))
      Console.setErr(new FileOutputStream(out))
    }

// **********  Uncomment only for debugging **********
//    val environmentVars = System.getenv
//    for ((k,v) <- environmentVars) println(s"key: $k, value: $v")
//    val properties = System.getPropertiesCRUpper
//    for ((k,v) <- properties) println(s"key: $k, value: $v")
    
//    println("java.library.path: "+System.getProperty("java.library.path"))
//    println("User name: "+System.getProperty("user.name"))
//    println("jna.loaded: "+System.getProperty("jna.loaded"))
//    println("jna.library.path: "+System.getProperty("jna.library.path"))
//    val env = System.getenv("LD_LIBRARY_PATH")
//    println("LD_LIBRARY_PATH: "+env)
//    val env2 = System.getenv("PATH")
//    println("PATH: "+env2)
    
//    val libgsl: Library = Library("gsl")
//    val y:Double = libgsl.gsl_sf_bessel_J0 (5)[Double]
//    println("GSL result: "+y)
// *********************************************************

    // Initialize strategies and DE parameters
    println(" Creating strategies and auxiliary objects...")
//    var params = new DEParameters(nElem, dim, fLower, fUpper, crLower, crUpper, target, evalFunc, mEval, min, max, workers, locI, maxRunTime=maxTime, minDistance=minDistance, solver=solverName)
    var params = DEParameters(config)
    
    var evaluator: Evaluator = Class.forName("deFunctions.evaluators."+params.evalName).newInstance().asInstanceOf[Evaluator]  // evaluation strategy (i.e. fitness function) 
    val mutator: Mutator = new StandardMutator()  // mutation strategy 
    val partitioner: Partitioner = new EnhancedRandomExactPartitioner(workers, params.popSize)  // migration strategy (random shuffle)
    // TODO: implement the strategy pattern in the "Scala way" (i.e. factory objects) for all previous components 

   // Initial "dummy" evaluation for benchmarks that internally change the experiment configuration
   // Needed to update params to reflect the changes
   if (evaluator.needsConfiguration()) evaluator(params)
   if (evaluator.needsCleanup()) evaluator.clean()

// **********  Uncomment only for debugging **********
//   println("jna.loaded: "+System.getProperty("jna.loaded"))
// *********************************************************
   
   val localsolver:Option[LocalSolver[Individuo]] = if (params.solver.isEmpty) None else { Some(new DELocalSolver(params)) }  // Optional local solver   
   // Note: LS has to be initialized after "dummy" evaluation to get the updated params


    println(params)
    println("-------------------------------------------------------------------------------")

    
    // Initiliaze Spark
    print("Spark initialization... ")
    val tSPInit_I = System.nanoTime()
    var conf = new SparkConf().setAppName(jobName)
    if (localRun)
      conf = conf.setMaster("local["+workers+"]")
    else
      conf = conf.set("spark.executor.instances", workers.toString()).set("spark.executor.extraJavaOptions", "-Xss32m")
      //conf = conf.set("spark.executor.extraLibraryPath", "/root/SparkFolder")
    
    System.setProperty("spark.executor.memory", "2000M")
    Logger.getLogger("org").setLevel(level)
    Logger.getLogger("akka").setLevel(level)
    val sc = new SparkContext(conf)

    
    // run first the sequential algorithm the given number of times, if so configured
    if (secFirst && secRuns>0){
    	println("**************  Sequential algorithm  **************")
      var secDe: Optimizer = new SequentialDE(params, evaluator, mutator, partitioner)  // instantiate the sequential algorithm with given parameters and strategies
      for (i <- 0 until secRuns){
        println("•·.·´`·.·•·.·´`·.·•·.·´`·.·•·.·´`·.·•  Run # "+(i+1)+"/"+secRuns+"  •·.·´`·.·•·.·´`·.·•·.·´`·.·•·.·´`·.·•")
        // configure the evaluator before execution if needed
        if (evaluator.needsConfiguration()) evaluator(params)
        // run the sequential DE
      	val tSec_I = System.nanoTime()
      	val resultado_sec: Individuo = secDe.run()
      	val tSec_F = System.nanoTime()
      	// log results
      	println("Sequential execution finished. Time: "+  (tSec_F-tSec_I)./(1000000000.0) + " s.")
      	if (resultado_sec.evaluation <= params.target){
      		println("Execution reaches the target value. Best result:")
      		println("Fitness function: "+resultado_sec.evaluation+" Target value: "+params.target)
      		println("Vector: "+resultado_sec.values.mkString("(", ", ", ")") )
      	} else {
      		println("Target value was not reached.")
      		println("Best result: "+resultado_sec.evaluation+" Target value: "+params.target)
      	}
        // clean the evaluator configuration after execution, if needed  
      	if (evaluator.needsCleanup()) evaluator.clean()
//        println("--------------------------------------------------")
        // store time results
        tiemposSec(i) = (tSec_F-tSec_I)./(1000000000.0)  
      }
    }
 
    
//evaluator = Class.forName("deFunctions.evaluators."+evalFunc).newInstance().asInstanceOf[Evaluator]
//    if (evaluator.needsConfiguration()) evaluator(params)
    
    println("************** Resultados de Spark **************")
    //Spark initialization
    val tSPInit_F = System.nanoTime()
    println("Done. Time: "+(tSPInit_F-tSPInit_I)./(1000000000.0)+" s.")
    //println("------ Resultados de Spark ------")

        
    // run the island-based parallel algorithm the given number of times    
    var sipde: Optimizer = new SparkIslandsDE(params, evaluator, mutator, partitioner, localsolver, sc)  // instantiate the island-based parallel algorithm algorithm with given parameters and strategies
    for (i <- 0 until parRuns){
        println("_____________________________________Run # "+(i+1)+"/"+parRuns+"_____________________________________")

// **********  Uncomment only for debugging **********
//   println("jna.loaded: "+System.getProperty("jna.loaded"))
// *********************************************************
        
      // run the island-based parallel DE 
      // Note: for parallel algorithm evaluator is configured inside run
      val tPar4_I = System.nanoTime()
      val resultado4_Spark: Individuo = sipde.run()
      val tPar4_F = System.nanoTime()
      // log results
      println("Island-based parallel execution finished. Time: "+ (tPar4_F-tPar4_I)./(1000000000.0) + " s.")
      if (resultado4_Spark.evaluation <= params.target){
        println("Execution finished. Best result:")
        println("Fitness function: "+resultado4_Spark.evaluation+" Target value: "+params.target)
        println("Vector: "+resultado4_Spark.values.mkString(" ") )
      } else {
        println("Target value was not reached.")
        println("Best result: "+resultado4_Spark.evaluation+" Target value: "+params.target)
      }   
      // clean the evaluator configuration after execution, if needed
      if (evaluator.needsCleanup()) evaluator.clean()      
      // store time results
      tiemposPar(i) = (tPar4_F-tPar4_I)./(1000000000.0)      
    }
    println("--------------------------------------------------")
    

    // run after the sequential algorithm the given number of times, if so configured
    if (!secFirst && secRuns>0){
      println("**************  Sequential algorithm  **************")
      var secDe: Optimizer = new SequentialDE(params, evaluator, mutator, partitioner)  // instantiate the sequential algorithm with given parameters and strategies
      for (i <- 0 until secRuns){
        println("•·.·´`·.·•·.·´`·.·•·.·´`·.·•·.·´`·.·•  Run # "+(i+1)+"/"+secRuns+"  •·.·´`·.·•·.·´`·.·•·.·´`·.·•·.·´`·.·•")
        // configure the evaluator before execution, if needed
        if (evaluator.needsConfiguration()) evaluator(params)
        // run the sequential DE        
        val tSec_I = System.nanoTime()
        val resultado_sec: Individuo = secDe.run()
        val tSec_F = System.nanoTime()
      	// log results  
        println("Sequential execution finished. Time: "+  (tSec_F-tSec_I)./(1000000000.0) + " s.")
      	if (resultado_sec.evaluation <= params.target){
      		println("Execution reaches the target value. Best result:")
      		println("Fitness function: "+resultado_sec.evaluation+" Target value: "+params.target)
      		println("Vector: "+resultado_sec.values.mkString("(", ", ", ")") )
      	} else {
      		println("Target value was not reached.")
      		println("Best result: "+resultado_sec.evaluation+" Target value: "+params.target)
      	}
        // clean the evaluator configuration after execution, if needed
      	if (evaluator.needsCleanup()) evaluator.clean()
        println("--------------------------------------------------")
        // store time results
        tiemposSec(i) = (tSec_F-tSec_I)./(1000000000.0)       
      }
    }
    
    
    // log resumed execution times
    println("Execution times")
    if (parRuns > 0){
      println("----------")
      println("Parallel with Spark:")
      println("Individual times: "+tiemposPar.mkString("[", ", ", "]"))
      var mediaPar = 0.0
      for (i <- 0 until parRuns){
        mediaPar += tiemposPar(i)
      }
      println("Mean:"+ mediaPar/parRuns)
    }
    
    if (secRuns>0){
      println("----------")
      println("Sequential:")
      println("Individual times: "+tiemposSec.mkString("[", ", ", "]"))
      var mediaSec = 0.0
      for (i <- 0 until secRuns){
        mediaSec += tiemposSec(i)
      }
      println("Mean:"+ mediaSec/secRuns)
    }
    
  }
}


