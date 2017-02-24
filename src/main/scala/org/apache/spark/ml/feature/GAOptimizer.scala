package org.apache.spark.ml.feature

import scala.util.Random
import scala.collection.immutable.VectorBuilder

import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner

abstract class PopulationGenerator[T](
  val islandPopulationSize: Int, val nIslands: Int, 
  val evaluator: StateEvaluator[T], val partitioner: Partitioner) extends Serializable {

  require(islandPopulationSize * nIslands < Int.MaxValue,
    "Total population size must be less than Int.MaxValue, since partitioners work with integer keys")

  require(islandPopulationSize % 2 == 0, 
    "Island population must be an even number")

  val populationSize = islandPopulationSize * nIslands

  def generate: RDD[EvaluatedState[T]]
}

abstract class Mutator[T] extends Serializable {
  def mutate(state: EvaluableState[T]): EvaluableState[T]
}

abstract class Matchmaker[T] extends Serializable {
  def crossover(stateA: EvaluableState[T], stateB: EvaluableState[T]): 
      (EvaluableState[T], EvaluableState[T])
}

// Implements search based on Genetic Algorithms 
class GAOptimizer[T](
  mutator: Mutator[T],
  matchmaker: Matchmaker[T],
  populationGenerator: PopulationGenerator[T],
  maxGenerations: Int,
  maxTimeHours: Double,
  minMerit: Double,
  eliteSize: Int,
  tournamentSize: Int) extends Optimizer[T] {

  require(eliteSize % 2 == 0, "Elite size must be an even number")

  var population: RDD[EvaluatedState[T]] = populationGenerator.generate
  var nGenerations: Int = 0
  var endSearch: Boolean = false
  var initTimeMillis: Long = _
  var best: EvaluatedState[T] = _
  var newBest: EvaluatedState[T] = _ 
  var bestMeritsLog: Vector[Double] = _
  
  // Shorten names
  val evaluator = populationGenerator.evaluator
  val nIslands = populationGenerator.nIslands
  val populationSize = populationGenerator.populationSize
  val islandPopulationSize = populationGenerator.islandPopulationSize 
  val partitioner = populationGenerator.partitioner

  def search: EvaluatedState[T] = {
    var bestMeritsLogBuilder: VectorBuilder[Double] = new VectorBuilder[Double]
    initTimeMillis = System.currentTimeMillis

    best = population.max
    bestMeritsLogBuilder += best.merit

    do {
      // DEBUG
      println("POPULATION")
      println(population.collect.mkString("\n"))

      evolveIslands
      nGenerations += 1

      // TODO cache population??
      // after the selectBest (reduce) action, the pop will be possible needed again for migration, also, is cache necessary if the population was produced from a SparkContext.range() shouldn't it be already on memory???
      population.cache

      newBest = population.max
      bestMeritsLogBuilder += newBest.merit

      // TODO Does this evaluation includes size??
      if(newBest.merit > best.merit){
        best = newBest
      }

      endSearch = 
        (isMaxTimeExceeded || 
          (nGenerations > maxGenerations) || 
          (best.merit >= minMerit))
      
      if(!endSearch) {
        migrateIndividuals
      }
    } while (!endSearch)

    bestMeritsLog = bestMeritsLogBuilder.result

    //DEBUG
    println("BEST MERITS LOG:")
    println(bestMeritsLog.mkString(", "))

    best
  }

  // private def selectBest: EvaluatedState[T] = {

  //   // TODO 
  //   // This approach is more efficient than making a map for evaluations and
  //   // then a reduce for selection, however it assumes that the evaluation
  //   // process is fast
  //   def betterInd(indA: Individual[T], indB: Individual[T]): Individual[T] = {
  //     if(evaluator.evaluate(indA) > evaluator.evaluate(indB))
  //       indA
  //     else 
  //       indB
  //   }

  //   val best = population.reduce(betterInd)

  //   new EvaluatedState[T](best, evaluator.evaluate(best))
  // }
  
  private def evolveIslands: Unit = {


    // def localEvolve(localPopulation:IndexedSeq[Individual[T]]): 
    def localEvolve(localPopulation:Iterator[EvaluatedState[T]]): 
      Iterator[EvaluatedState[T]] = {

      val indexedLocalPopulation = localPopulation.toIndexedSeq.sorted
      
      def tournamentSelection = {
        val tournamentMembers: IndexedSeq[EvaluatedState[T]] = 
          ((1 to tournamentSize)
            .map{ _ => 
              indexedLocalPopulation(Random.nextInt(islandPopulationSize)) }
          )
        
        tournamentMembers.max
      }

      val elite: IndexedSeq[EvaluatedState[T]] = 
        indexedLocalPopulation.slice(
          islandPopulationSize - eliteSize, islandPopulationSize)

      val parents: IndexedSeq[(EvaluableState[T],EvaluableState[T])] = 
        ((1 to (islandPopulationSize - eliteSize) / 2)
          .map( _ => (tournamentSelection.state, tournamentSelection.state) )
        )

      val children: IndexedSeq[EvaluableState[T]] = 
        (parents
          .map{ case (i,j) => matchmaker.crossover(i,j) }
          .flatMap{ case (i,j) => IndexedSeq(i, j) }
        )

      (elite ++ children.map{ ch =>
        val mutated = mutator.mutate(ch)
        new EvaluatedState(mutated, evaluator.evaluate(mutated)) 
      }).toIterator
    }

    population = 
      population.mapPartitions(localEvolve, preservesPartitioning=true) 

  }

  private def migrateIndividuals: Unit = {

    // DEBUG
    // println("POPULATION")
    // println(population
    //         .zipWithIndex.map{ case (ind, idx) => (idx, ind) }.collect.mkString(", "))

    // TODO test which approach is more efficient
    population = (population
      .zipWithIndex.map{ case (ind, idx) => (idx, ind) }
      .partitionBy(partitioner)
      // Drop indexes
      .map(_._2)
    )
    //  population = (population.context.range(0, populationSize)
    //   .zip(population)
    //   .partitionBy(partitioner)
    //   // Drop indexes
    //   .map(_._2)
    // )
  }

  private def isMaxTimeExceeded: Boolean = {
    val elapsedHours: Double = 
      (System.currentTimeMillis - initTimeMillis) / (1000d*3600d) 

    (elapsedHours > maxTimeHours)
  }

}