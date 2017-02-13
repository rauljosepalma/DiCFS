package org.apache.spark.ml.feature

import scala.util.Random
import scala.collection.immutable.VectorBuilder

import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner

abstract class Individual[T] extends EvaluableState[T] {
  def evaluableData = genotype
  def genotype: T
  def genotype_= (g: T): Unit
} 

abstract class PopulationGenerator[T](
  val islandPopulationSize: Int, val nIslands: Int, val partitioner: Partitioner) {

  require(islandPopulationSize % 2 == 0, 
    "Island population must be an even number")

  val populationSize = islandPopulationSize * nIslands


  def generate: RDD[Individual[T]]
}

abstract class Mutator[T] {
  def mutate(ind: Individual[T]): Individual[T]
}

abstract class Matchmaker[T] {
  def crossover(indA: Individual[T], indB: Individual[T]): 
      (Individual[T], Individual[T])
}

// Implements search based on Genetic Algorithms 
class GAOptimizer[T](
  evaluator: StateEvaluator[T],
  mutator: Mutator[T],
  matchmaker: Matchmaker[T],
  populationGenerator: PopulationGenerator[T],
  maxGenerations: Int,
  maxTimeHours: Double,
  minMerit: Double) extends Optimizer(evaluator) {

  var population: RDD[Individual[T]] = populationGenerator.generate
  var nGenerations: Int = 0
  var endSearch: Boolean = false
  var initTimeMillis: Long = _
  var best: Individual[T] = _
  var bestMerit: Double = _
  var newBest: Individual[T] = _ 
  var newBestMerit: Double = _
  var bestMeritsLog: Vector[Double] = _
  
  // Just to shorten names
  val nIslands = populationGenerator.nIslands
  val populationSize = populationGenerator.populationSize
  val islandPopulationSize = populationGenerator.islandPopulationSize 
  val partitioner = populationGenerator.partitioner

  def search: EvaluableState[T] = {
    var bestMeritsLogBuilder: VectorBuilder[Double] = new VectorBuilder[Double]
    initTimeMillis = System.currentTimeMillis

    best = selectBest
    bestMerit = evaluator.evaluate(best) 
    bestMeritsLogBuilder += bestMerit

    do {
      evolveIslands
      
      // TODO cache population??
      // after the selectBest (reduce) action, the pop will be possible needed again for migration, also, is cache necessary if the population was produced from a sc.range() shouldn't it be already on memory???
      population.cache

      newBest = selectBest
      newBestMerit = evaluator.evaluate(newBest) 
      bestMeritsLogBuilder += newBestMerit

      if(newBestMerit <= bestMerit){
        nGenerations += 1
      } else {
        best = newBest
        bestMerit = newBestMerit
      }

      endSearch = 
        ( isMaxTimeExceeded || 
          (nGenerations > maxGenerations) || (bestMerit >= minMerit) )
      
      if(!endSearch) {
        migrateIndividuals
      }
    } while (!endSearch)

    bestMeritsLog = bestMeritsLogBuilder.result

    best
  }

  private def selectBest: Individual[T] = {

    def betterInd(indA: Individual[T], indB: Individual[T]): Individual[T] = {
      if(evaluator.evaluate(indA) > evaluator.evaluate(indB))
        indA
      else 
        indB
    }

    population.reduce(betterInd)
  }
  
  private def evolveIslands: Unit = {

    // def localEvolve(localPopulation:IndexedSeq[Individual[T]]): 
    def localEvolve(localPopulation:Iterator[Individual[T]]): 
      Iterator[Individual[T]] = {

      val indexedLocalPopulation = localPopulation.toIndexedSeq
      val parentsIndexes: IndexedSeq[(Int, Int)] = 
        ((0 to islandPopulationSize / 2)
          .map {
          _ => (Random.nextInt(islandPopulationSize),
                Random.nextInt(islandPopulationSize))
          }
        )
      val parents: IndexedSeq[(Individual[T],Individual[T])] = 
        (parentsIndexes
          .map{ case (i,j) => (indexedLocalPopulation(i), 
                               indexedLocalPopulation(j)) }
        )
      val children: IndexedSeq[Individual[T]] = 
        (parents
          .map { case (i,j) => matchmaker.crossover(i,j) }
          .flatMap { case (i,j) => IndexedSeq(i, j) }
        )

      children.map(mutator.mutate(_)).toIterator

    }

    population = 
      population.mapPartitions(localEvolve, preservesPartitioning=true) 

  }

  private def migrateIndividuals: Unit = {

    // TODO test which approach is more efficient
    // population.zipWithIndex.map{ case (ind idx) => (idx, ind) }.
     population = (population.context.range(0, populationSize)
      .zip(population)
      .partitionBy(partitioner)
      // Drop indexes
      .map(_._2)
    )
  }

  private def isMaxTimeExceeded: Boolean = {
    val elapsedHours: Double = 
      (System.currentTimeMillis - initTimeMillis) / (1000d*3600d) 

    (elapsedHours > maxTimeHours)
  }

}