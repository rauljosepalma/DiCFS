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
   *  	File:   SparkIslandsDE.scala
   * 		Author: Diego Teijeiro <diego.teijeiro@udc.es>
   *  	Contributor: Xoan C. Pardo <xoan.pardo@udc.gal>
   * 
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
   * 		Implementation of the island-based version of the DE algorithm
   * 
   *  *****************************************************************
   */

package deFunctions



import deFunctions.evaluators.Evaluator
import deFunctions.mutators.Mutator
import org.apache.spark.Partitioner
import deFunctions.solvers.LocalSolver
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import annotation.meta.field
import deFunctions.solvers.AsyncSolver
import org.apache.commons.math3.random.RandomDataGenerator
import deSparkFunctions.FunctionHolder
import deFunctions.partitioners.EnhancedRandomExactPartitioner
import sna.Library
import scala.collection.immutable.Vector
import scala.collection.mutable.ListBuffer
import deFunctions.mutators.DEMutatorParams


/**
 * @author diego
 * @author xoan
 * 
 * 
 * @param params Parameters of the experiment
 * @param eval Evaluation strategy
 * @param mutator Mutation strategy
 * @param partitioner Migration strategy (random shuffle) -> not used at the moment (a fixed custom partitioner is used)
 * @param ls Optional Local Solver (if defined, a local search is run asynchronously with the islands evolution) 
 * @param sc SparkContext
 * 
 */
class SparkIslandsDE(val params: DEParameters, val eval:Evaluator, val mutator:Mutator, val partitioner:Partitioner, ls:Option[LocalSolver[Individuo]], @(transient @field) val sc:SparkContext) extends Optimizer{
  
  var tabuList = List[Individuo]()  // taboo list used with local search
  @transient
  val localSolver:Option[AsyncSolver[Individuo]] = if (ls.isDefined) {Some(new AsyncSolver(ls.get))} else None // wraps the local solver into an asynchronous task
  
  //Spark accumulators (used to aggregate island execution times and number of fitness evaluations)
  val timeAccum = sc.accumulator[Long](0, "Time_mappers")
  val evalsAccum = sc.accumulator(0, "Evals_last_Island")
  val time1Accum = sc.accumulator[Long](0, "Time_part_1")
  val time2Accum = sc.accumulator[Long](0, "Time_part_2")
  val time3Accum = sc.accumulator[Long](0, "Time_part_3")
  
  /**
   * 
   *  Island evolution. Function to be executed in each partition (i.e. island)
   *  
   *  @param lst Iterator to island individuals
   *  @param parameterTuple The F and CR parameters of the DE algorithm
   *  																				 (the same for all islands in homogeneous configurations, different in heterogeneous)
   */
  val islandsMaps = (lst:Iterator[(Int, Individuo)], parameterTuple:(Double, Double)) => 
          {
// **********  Uncomment only for debugging **********            
//            println("java.library.path: "+System.getProperty("java.library.path"))
//            println("jna.library.path: "+System.getProperty("jna.library.path"))
//            System.setProperty("jna.library.path",  "/home/diego.teijeiro/SparkDE/libs:/usr/lib64")
//            System.setProperty("jna.debug_load",  "true")
//            println("jna.library.path: "+System.getProperty("jna.library.path"))
//            println("User name: "+System.getProperty("user.name"))
//            println("jna.loaded: "+System.getProperty("jna.loaded"))
//            val env = System.getenv("LD_LIBRARY_PATH")
//            println("LD_LIBRARY_PATH: "+env)
//            val env2 = System.getenv("PATH")
//            println("PATH: "+env2)
// *********************************************************
            
// **********  Uncomment only for debugging **********                        
//            val libgsl: Library = Library("gsl")
//            val y:Double = libgsl.gsl_sf_bessel_J0 (5)[Double]
//            println("GSL result: "+y)
//            for (Map.Entry[String, String] entry : env.entrySet()){
//              
//            }
//            for ( (k,v) <- env ) println("key: "+k+", value: "+v)
//            val properties = System.getProperty("java.library.path")
//            println("java.library.path: "+properties)
////            for ((k,v) <- properties) println(s"key: $k, value: $v")
//            val jna = System.getProperty("jna.library.path")
//            println("jna.library.path: "+properties)
// *********************************************************
 
// **********  Uncomment only for debugging **********                        
//            println("\n||||DE configuration: F: "+parameterTuple._1+" CR: "+parameterTuple._2)
// *********************************************************
            
            // aux vars (local to each island)
            var parada = false  // stopping condition
            var iterLoc = 0        // Number of local iterations (i.e. isolated island evolutions)
            var localEvals = 0   // Number of fitness evaluations 
            val rand:RandomDataGenerator = new RandomDataGenerator()
                        
// **********  Uncomment only for debugging **********                        
//            val pw = new PrintWriter(new BufferedWriter(new FileWriter("evals.txt", true)))
// *********************************************************
            
            // configure the evaluator before execution if needed
            if (eval.needsConfiguration()) {
               eval(params) 
            }
//            if (eval.needsCleanup()) eval.clean()
//            if (eval.needsConfiguration()) eval(parameters)
        
            var oldPop = lst.toVector  // current island population
            var newPop: Vector[(Int, Individuo)] = Vector()  // new island population
            val localIter_I = System.nanoTime()
            
            // main loop (until the configured number of local iterations was reached)
            while (!parada){
              iterLoc += 1
              // initialize new population
              newPop = Vector()
              // for each individual calculate a new solution
              for (i <- 0 until oldPop.size){
                val time1_I = System.nanoTime()
                // pick five different random individuals from population 
                // Note: this is to support both DE/Rand/1 and DE/Rand/2 mutation strategies                
                var father:Individuo = oldPop(i)._2
                var indA = rand.nextInt(0, oldPop.size-1)
                while (indA == i)
                  indA = rand.nextInt(0, oldPop.size-1)
                var indB = rand.nextInt(0, oldPop.size-1)
                while (indB==indA || indB == i)
                  indB = rand.nextInt(0, oldPop.size-1)
                var indC = rand.nextInt(0, oldPop.size-1)
                while (indC==indB || indC == indA || indC == i)
                  indC = rand.nextInt(0, oldPop.size-1)
                var indD = rand.nextInt(0, oldPop.size-1)
                while (indD==indC || indD==indB || indD == indA || indD == i)
                  indD = rand.nextInt(0, oldPop.size-1)
                var indE = rand.nextInt(0, oldPop.size-1)
                while (indE==indD || indE==indC || indE==indB || indE == indA || indE == i)
                  indE = rand.nextInt(0, oldPop.size-1)
                val fathers = new ListBuffer[Individuo]()
                fathers.+=(father)
                fathers.+=(oldPop(indA)._2)
                fathers.+=(oldPop(indB)._2)
                fathers.+=(oldPop(indC)._2)
                fathers.+=(oldPop(indD)._2)
                fathers.+=(oldPop(indE)._2)
                
//                var a :Individuo = oldPop(indA)._2
//                var b :Individuo = oldPop(indB)._2
//                var c :Individuo = oldPop(indC)._2
//                var d :Individuo = oldPop(indD)._2
//                var e :Individuo = oldPop(indE)._2
                
                // Apply mutation strategy to generate a new individual (candidate solution)
//                var child:Individuo = mutator.mutate(params, father, a, b, c)
                var child:Individuo = mutator.mutate(new DEMutatorParams(params.nP, parameterTuple._1, parameterTuple._2), fathers.toList)
                var vector = child.values
                
                time1Accum.add(System.nanoTime()-time1_I)
                val time2_I = System.nanoTime()
                
// **********  Uncomment only for debugging **********               
//    for (i <- 0 until vector.length){
//      if (vector(i)<params.lowLimit){
//        println("Position "+i+" below lower limit. Vector: "+vector.mkString("[", ",", "]"))
//      }
//      if ( vector(i)>params.upLimit){
//        println("Position "+i+" above upper limit. Vector: "+vector.mkString("[", ",", "]"))
//      }
//    }
// *********************************************************

                 // Evaluate the new candidate solution
                child.evaluate(eval.evaluate)
                
// **********  Uncomment only for debugging **********               
//                println("Evaluation: "+child.evaluation)
// *********************************************************
                
                time2Accum.add(System.nanoTime() - time2_I)
                
                val time3_I = System.nanoTime()
                localEvals+=1
                
// **********  Uncomment only for debugging **********               
//                pw.println(child.toString())
// *********************************************************
                
                 // Replace the original if the new candidate solution is better
                if (child.evaluation<father.evaluation)
                  newPop = newPop.:+(oldPop(i)._1, child)
                else
                  newPop = newPop.:+(oldPop(i)._1, father)
                  
                // Check stopping criterion: new candidate solution reached target value?
                if (child.evaluation<=params.target){
                  parada = true
                  
                }
                time3Accum.add(System.nanoTime() - time3_I)
                               
              }

             // Check stopping criterion: local iterations reached maximum value?
              if (!parada)
                parada = iterLoc>=params.localIter
                
              // Update current population
              oldPop = newPop
            }
            
            // Update accumulators (execution time and number of fitness evaluations)
            val localIter_F = System.nanoTime()
            timeAccum.add(localIter_F-localIter_I)
            evalsAccum.add(localEvals)
            
// **********  Uncomment only for debugging **********
//            for (element <- newPop) {
//              println("{"+element._1+", ["+element._2.values.mkString(", ")+"], V:"+ element._2.evaluation +"}")
//              
//            }
// *********************************************************
            
            // clean the evaluator configuration after execution, if needed
//            eval.needsCleanup()
            if (eval.needsCleanup()) eval.clean()
            
            // return an iterator to the new island population
            newPop.iterator
//            oldPop.iterator            
      }

/**
 * 
 *  Calculate euclidean distance between 2 individuals (used in taboo list)
 *  
 */
  def distancia_euclidea (a:Individuo, b:Individuo): Double = {
    
    def square(a: Double) = a * a      // square function
    
    var distSum:Double = 0
    //iterate over the two vectors
    for (i <- 0 until params.nP){
      distSum += square(a.values(i) - b.values(i) ) 
    }
    return scala.math.sqrt(distSum)
  }
  
  /**
   * 
   *   The method that executes the algorithm
   *   
   */  
  def run():Individuo = {
    // aux vars
    var nEval:Int = 0  // number of evaluations
    var gen:Int = 0   // generation counter
    var nShuffles:Int = 1  // migration counter
    var totalShuffleTime: Long = 0  // migration time
    var bestValue:Double = Double.MaxValue  // best fitness value
    var nSolvers: Int = 0  // Local search runs counter
    var solverFinished: Boolean = false  // Flag to indicate if local search is running (a workaround to recuperate from LS hangs)
    
    // Time-related aux vars
    var bestRunTime:Long = 0
    val runTime_I: Long = System.nanoTime()
    var runTime:Long = 0
    // Reset the accumulator for the number of evaluations 
    evalsAccum.setValue(evalsAccum.zero)
    
    println("Executing with Spark, cooperative island-based algorithm.")    
// **********  Uncomment only for debugging **********
//    println("ID thread principal: " + Thread.currentThread.getId())
// *********************************************************

    tabuList = List[Individuo]()  // Init taboo list
    val tSparkLoad_I = System.nanoTime()

     // Initiate a function holder (used to group functions passed to Spark to be executed in the workers)
//    var functionHolder: FunctionHolder = FunctionHolder(eval, mutator, parameters)
    var functionHolder: FunctionHolder = new FunctionHolder(eval, mutator, params)
    
    // Configure the evaluator before execution, if needed
    if (eval.needsConfiguration()) eval(params)
    println("Evaluator configured in the driver.")

    // Create the DE configuration parameters (F and CR) for each island (heterogeneous configuration)
    val r: RandomDataGenerator = new RandomDataGenerator()    
    val islandParameters = (0 to params.workers-1).map(i => {
      //generate 2 random indexes. 
      //one in the range [0,nF]
      val fIndex = if (params.f.size==1) {0} else {r.nextInt(0, params.f.size-1)}
      //the other in the range [0,nCR]
      val crIndex = if (params.cr.size==1) {0} else {r.nextInt(0, params.cr.size-1)}
      (params.f(fIndex), params.cr(crIndex))
    }).toArray    
    // Print CR values
    println(islandParameters.mkString("Island DE parameters: \n\t", "\n\t", "\n"))

    // Initialize (in the workers -> distributed version) and distribute random population with default partitioner 
    // (distributed version only worths evaluating costly fitness functions)
//    var rdd = sc.parallelize(0 until parameters.popSize, params.workers).map(i => (i, new Individuo(parameters, evalFunc = eval.evaluate)))
    
    // Initialize (in the driver -> not distributed version) and distribute random population with our custom partitioner
    var initPop: Array[(Int, Individuo)] = new Array [(Int, Individuo)](0)
    for (i <- 0 until params.popSize){
      var ind = new Individuo(params, evalFunc=eval.evaluate) // create and evaluate the individual using the fitness function of the evaluation strategy
//      var ind = new Individuo(parameters, el1, evalFunc = eval.evaluate)
      initPop = initPop.+:((i, ind)) 
    }
    var rdd = sc.parallelize(initPop)
    rdd = rdd.partitionBy(new EnhancedRandomExactPartitioner (params.workers, params.popSize))     // Distribute the population using our custom partitioner
    
    rdd.cache()  // persist partitions in RAM

// **********  Uncomment only for debugging **********   
//    // Show elements
//    for (element <- initPop) {
//      println("{"+element._1+", ["+element._2.values.mkString(", ")+"], V:"+ element._2.evaluation +"}")
//      
//    }
// *********************************************************

    nEval+=params.popSize  // update number of evaluations

// **********  Uncomment only for debugging **********   
//    for (element <- rdd.collect()) {
//      println("("+element._1+", |"+element._2.values.mkString(", ")+"|")
//      
//    }
// *********************************************************
    
    // Initialize aux vars
    var parada = false  // stopping condition
    var localSolverConverged = false  // Local search stopping condition
    val tSparkLoad_F = System.nanoTime()

    // main loop 
    while (!parada) {
      
        println("»»------------- Stage "+nShuffles+" -------------««")
        
// **********  Uncomment only for debugging **********   
//      val islandTime_I: Long = System.nanoTime()
      
//      val keys = rdd.keys.collect()
//      println("keys: "+keys.mkString(", "))
//      val evals = rdd.collect()
//      var evalsSt = ""
//      for (i<- 0 until evals.length){
//        evalsSt+= evals(i)._2.evaluation+", "
//      }
//      println("Evals: "+evalsSt)
// *********************************************************
        
      // Initate a local search, if defined and not yet launched
      if (localSolver.isDefined && !localSolver.get.isRunning) {
         // Get the best candidate solution far enough away from solutions in the taboo list  
        val tabuListExists = (i:(Int, Individuo)) => !( tabuList.exists(t => distancia_euclidea(t, i._2) <= params.minDistance))
        val filteredRDD = if (tabuList.isEmpty) { rdd.collect() } else { rdd.filter(tabuListExists).collect() }  // filter solutions by euclidean distance      
        if (!filteredRDD.isEmpty) {
          val getIndividuos = (f:(Int, Individuo)) => f._2
          val individuos = filteredRDD.map(getIndividuos)
          val bestInd = individuos.min(IndividuoOrdering)  // get the best candidate

          // Add the best candidate to the taboo list 
          tabuList = tabuList.:+(bestInd)
          println("###Taboo List:\n###" + tabuList.mkString("\n###") + "\n###End of Taboo List.")
          
          // Launch an asynchronous local search starting at the best candidate
          nSolvers+=1
          solverFinished = false
          localSolver.get.run(bestInd)
        } else {
          println("###No points valid for local search.")
          solverFinished = true
        }
      } else if (localSolver.isDefined && localSolver.get.isRunning){
        println("###Local search still running.")
      }
         
      // Evolve islands (each island evolves isolated during a configurable number of evolutions)
      val tBlock_I = System.nanoTime()
      rdd = rdd.mapPartitionsWithIndex((index, iter) => (islandsMaps(iter, islandParameters(index))))

      rdd.cache() // persist new islands in RAM
      
      // Select the best solution found until now
      bestValue = rdd.reduce( functionHolder.finalReduction )._2.evaluation
      val tBlock_F = System.nanoTime()
      
      // Update the counter of fitness evaluations
      // (every island makes localIter*(popSize/workers) evaluations, and number of islands = number of workers)
      nEval += params.localIter*params.popSize

      // log evolution
      println("\nNumber of fitness evaluations: "+nEval+" ( "+ evalsAccum.value + " exact value in mappers). bV: "+bestValue)
      println("\tBlock time: "+(tBlock_F-tBlock_I)./(1000000000.0) + " s. Mappers mean time: "+ (timeAccum.value./(params.workers))./(1000000000.0)+" s.")
      println("\tTime from the beginning: "+(System.nanoTime() - runTime_I)./(1000000000.0) + " s.")

// **********  Uncomment only for debugging **********
//      println("\tMean time of part 1: "+(time1Accum.value/params.workers)./(1000000000.0) + " s. TMean time of part 2: "+(time2Accum.value/params.workers)./(1000000000.0) +" s. Mean time of part 3: "+(time3Accum.value/params.workers)./(1000000000.0) +" s.") 
// *********************************************************
      
      // Check for an improved local search result 
      if (localSolver.isDefined && localSolver.get.isCompleted && !solverFinished && localSolver.get.value.evaluation < localSolver.get.initValue.evaluation) {
        val lsresult = localSolver.get.value  // get the local search result
        println("\n###Adding the LS result: " + lsresult)
        tabuList = tabuList.:+(lsresult)  // Add it to the taboo list

// **********  Uncomment only for debugging **********        
//        println("###Taboo List:\n###" + tabuList.mkString("\n###") + "\n###Fin Taboo List.")
// *********************************************************

        solverFinished = true

        // Check stopping criterion: local solver solution reached target value?
        if (!parada) parada = (lsresult.evaluation <= params.target)
        
        // Update the best value when the algorithm converges by the LS and the LS result is the best
        if (parada && lsresult.evaluation <= bestValue) {     
          println("###The algorithm converges by the LS.")
          bestValue = lsresult.evaluation
          localSolverConverged = true
        } else {   
          // Apply the substitution strategy when the algorithm does not converge by the LS
          // Note: a fixed strategy is used: the worst individual from each island is replaced by the LS result if it is better 
          // TODO: implement more substitution strategies using the strategy pattern         
          val mapSubstitucion = (p:Iterator[(Int, Individuo)]) =>  {  // define the substitution strategy
             val elements = p.toArray
             val worst = elements.maxBy({_._2})(IndividuoOrdering)
              if (lsresult.evaluation < worst._2.evaluation) {
                 val index = elements.indexOf(worst)
                 elements(index) = (worst._1, lsresult)
              }
             elements.iterator
           }
          rdd = rdd.mapPartitions(mapSubstitucion, preservesPartitioning=true).cache  // apply the substitution strategy and persist the result in RAM
        }
      }
      
// **********  Uncomment only for debugging **********        
//      println("[DEBUG]isDefined: "+ localSolver.isDefined + " && !isRunning: " + (!localSolver.get.isRunning) )
//      println("[DEBUG]isDefined: "+ localSolver.isDefined + " && isRunning: " + localSolver.get.isRunning )
//      println("[DEBUG]isDefined: "+ localSolver.isDefined + " && isCompleted: " + localSolver.get.isCompleted +" && eval: " + (localSolver.get.value.evaluation < localSolver.get.initValue.evaluation))
// *********************************************************
      
      gen+=params.localIter  // update generation counter

      // Apply the naive migration strategy (random shuffle) using our custom partitioner (only if stopping criteria are false)
      // TODO: implement more migration strategies using the strategy pattern         
      if (bestValue>params.target && nEval<params.maxEval){
        val tShuffle_I = System.nanoTime()
  //      rdd = rdd.repartition(params.workers)
        rdd = rdd.partitionBy(new EnhancedRandomExactPartitioner (params.workers, params.popSize))
        rdd.cache() // persist the result in RAM (forces RDD lazy evaluation)       
        val tShuffle_F = System.nanoTime()       
        nShuffles+=1  // Update the migrations counter
        println("\tShuffle # "+nShuffles + " Time to move data: "+(tShuffle_F-tShuffle_I)./(1000000000.0) + " s.")
        totalShuffleTime += tShuffle_F-tShuffle_I
      }
      

      // Check stopping criterion (distributed): any candidate solution reached target value?
      val orReduction = (a:Boolean, b:Boolean)=> {a || b}
      parada = (rdd.map[Boolean]( functionHolder.checkStop ).reduce(orReduction) || parada)

      // Update time counters
      val tIter_F = System.nanoTime()
      val iterTime = tIter_F-tBlock_I
      if (parada){
        bestRunTime+=timeAccum.value
      } else {
        bestRunTime+=iterTime 
      }
      
      // Check stopping criterion: number of evaluations reached maximum allowed?
      if (nEval>=params.maxEval){
        parada=true
        println("Stopping execution because the max. number of evaluations was reached.")
      }
        
      // Check stopping criterion: execution time reached maximum allowed?
      runTime = System.nanoTime() - runTime_I  
      if (runTime > params.maxRunTime*1000000000.0){
        parada=true
        println("Stopping execution because the max. execution time was reached.")
      }

      // Reset Spark accumulators
      timeAccum.setValue(timeAccum.zero)
      time1Accum.setValue(time1Accum.zero)
      time2Accum.setValue(time2Accum.zero)
      time3Accum.setValue(time2Accum.zero)
 
    }

// **********  Uncomment only for debugging **********
//    val evals = rdd.collect()
//    var evalsSt = ""
//    for (i<- 0 until evals.length){
//      evalsSt+= evals(i)._2.evaluation+", "
//    }
//    println("Evals: "+evalsSt)
// *********************************************************
    
    // Get the best candidate solution (LS or population minimun)
    val bestSol:(Int, Individuo) = if (localSolverConverged) { (-1, localSolver.get.value)} else { rdd.reduce( functionHolder.finalReduction ) }
    
    // log results    
    println("Number of fitness evaluations: "+nEval+" ( "+ evalsAccum.value + " exact value in mappers) Number of generations: "+gen)
    println("Number of migrations (shuffles): " + nShuffles)
    println("Number of local searches: "+nSolvers)
    println("Time to initialize Spark: "+(tSparkLoad_F-tSparkLoad_I)./(1000000000.0) + " s.")
    println("Total time moving data: "+ totalShuffleTime./(1000000000.0) + " s.")
    println("Best solution: bV:"+bestSol._2.evaluation+" Target value: "+params.target)
    if (bestSol._2.evaluation <= params.target){
      if (localSolverConverged) {println("«[The solution was found by the LS.]»") } 
      else {println("«[The solution was found by the islands evolution.]»")}
      println("Time to find the solution "+bestRunTime /(1000000000.0) + " s.")
    } else{
      println("The solution did not reach the target, stopping by number of fitness evaluations.")
      println("the best solution is:")
      println(bestSol._2.toString())
    }
      
    // Return the best candidate solution found
    return bestSol._2
    
  }
}