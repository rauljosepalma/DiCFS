package org.apache.spark.ml.feature

import scala.util.Random
import scala.collection.immutable.BitSet

import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner
import org.apache.spark.SparkContext

// By extending Individual we also extend EvaluableState
class FeaturesSubset(var feats: BitSet, val nFeats: Int) 
  extends Individual[BitSet] {

  def genotype: BitSet = feats
  def genotype_= (g: BitSet) { feats = g }

  // Returns an indexed sequence of all possible new subsets with one more feat
  def expand: IndexedSeq[EvaluableState[BitSet]] = {
    (0 until nFeats)
      .filter { !this.feats.contains(_) } 
      .map { f => FeaturesSubset(this.feats + f, nFeats) }
  }

}

object FeaturesSubset {

  // A companion object had to be created because class constructor overloading
  // would require sending a non-def parameter to the original constructor and
  // this is not permited. Also case classes don't allow apply method
  // overloading.
  def apply(nFeats: Int) = new FeaturesSubset(randomGenotype(nFeats), nFeats)
  def apply(feats: BitSet, nFeats: Int)  = new FeaturesSubset(feats, nFeats)

  // Generates a random FeaturesSubset
  private def randomGenotype(maxSize: Int): BitSet = {
    require(maxSize > 1, "Individual's maxSize must be > 1")
    
    val setSize = Random.nextInt(maxSize) + 1

    def randomBitSet(set: BitSet): BitSet = {
      if(set.size < setSize) {
        randomBitSet(set + (Random.nextInt(maxSize) + 1))
      } else {
        set
      }
    }
    
    randomBitSet(BitSet.empty)
  }
}

class FeaturesSubsetPopulationGenerator(
  islandPopulationSize: Int, nIslands: Int, partitioner: Partitioner, 
  val nFeats: Int, sc:SparkContext)
  extends PopulationGenerator[BitSet](
    islandPopulationSize, nIslands, partitioner) {
  
  def generate: RDD[Individual[BitSet]] = {

    val indexedPopulation = 
      sc.range(0, populationSize).map(i => (i, FeaturesSubset(nFeats))).partitionBy(partitioner)

    // Drop indexes (they're only needed for partitioning)
    indexedPopulation.map(_._2)
  }
}

class FeaturesSubsetMutator(var mutationProbability: Float = 0.0F) 
  extends Mutator[BitSet] {

  def mutate(ind: Individual[BitSet]): Individual[BitSet] = {

    // Copy the original Individual
    val fs = new FeaturesSubset(
      BitSet.fromBitMaskNoCopy(ind.genotype.toBitMask), 
      ind.asInstanceOf[FeaturesSubset].nFeats)

    if (mutationProbability == 0.0F) {
      mutationProbability = 1.0F/fs.nFeats
    }
    for(i <- 0 until fs.nFeats) {
      if(mutationProbability >= Random.nextFloat){
        // TODO test if the are improvements using a mutable BitSet!
        fs.genotype = 
          if(fs.genotype.contains(i))
            fs.genotype - i
          else
            fs.genotype + i
      }
    }

    fs
  }
}

class UniformFeaturesSubsetMatchmaker(var swapProbability: Float = 0.0F) extends Matchmaker[BitSet] {

  def crossover(indA: Individual[BitSet], indB: Individual[BitSet]): 
    (Individual[BitSet], Individual[BitSet]) = {

    // Copy original Individuals    
    val fsA = new FeaturesSubset(
      BitSet.fromBitMaskNoCopy(indA.genotype.toBitMask), 
      indA.asInstanceOf[FeaturesSubset].nFeats)
    val fsB = new FeaturesSubset(
      BitSet.fromBitMaskNoCopy(indB.genotype.toBitMask), 
      indB.asInstanceOf[FeaturesSubset].nFeats)

    if (swapProbability == 0.0F) {
      swapProbability = 1.0F/fsA.nFeats
    }
    for(i <- 0 until fsA.nFeats) {
      if(swapProbability >= Random.nextFloat){
        val isInFsA = fsA.genotype.contains(i)
        val isInFsB = fsB.genotype.contains(i)
        if (isInFsA && !isInFsB) {
          println("Making swap!")
          fsA.genotype = fsA.genotype - i
          fsB.genotype = fsB.genotype + i
        } else if (isInFsB && !isInFsA){
          fsB.genotype = fsB.genotype - i
          fsA.genotype = fsA.genotype + i
        }
      }
    }

    println(fsA.genotype)
    println(fsB.genotype)

    (fsA, fsB)
  }
}