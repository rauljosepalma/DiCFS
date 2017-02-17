package org.apache.spark.ml.feature

import scala.util.Random
import scala.collection.immutable.BitSet

import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner
import org.apache.spark.SparkContext

class FeaturesSubset(var feats: BitSet, val nFeats: Int) 
  extends EvaluableState[BitSet] {

  def data: BitSet = feats
  def data_= (d: BitSet) { feats = d }

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
  def apply(nFeats: Int) = new FeaturesSubset(randomFeats(nFeats), nFeats)
  def apply(feats: BitSet, nFeats: Int)  = new FeaturesSubset(feats, nFeats)

  // Generates a random FeaturesSubset
  private def randomFeats(nFeats: Int): BitSet = {
    require(nFeats > 1, "FeaturesSubset's nFeats must be > 1")
    
    val setSize = Random.nextInt(nFeats) + 1

    def randomBitSet(set: BitSet): BitSet = {
      if(set.size < setSize) {
        randomBitSet(set + Random.nextInt(nFeats))
      } else {
        set
      }
    }
    
    randomBitSet(BitSet.empty)
  }
}

class FeaturesSubsetPopulationGenerator(
  islandPopulationSize: Int, nIslands: Int, 
  evaluator: StateEvaluator[BitSet], partitioner: Partitioner,
  val nFeats: Int)
  extends PopulationGenerator[BitSet](
    islandPopulationSize, nIslands, evaluator, partitioner) {

  // Generates a single size element for each feature (randomly repeats if
  // necesary)
  def generate: RDD[EvaluatedState[BitSet]] = {
    require(islandPopulationSize * nIslands >= nFeats, 
      "Total population size: %d must be >= than the number of feats".format(islandPopulationSize * nIslands))

    val indexedPopulation = 
      (SparkContext.getOrCreate.range(0, populationSize)
        .map{ i => 
          val fs = 
            if (i < nFeats)
              FeaturesSubset(BitSet(i.toInt), nFeats)
            else
              FeaturesSubset(BitSet(Random.nextInt(nFeats)), nFeats)

          (i, new EvaluatedState[BitSet](fs, evaluator.evaluate(fs)))
        }
        .partitionBy(partitioner)
      )
    // Drop indexes (they're only needed for partitioning)
    indexedPopulation.map(_._2)
  }

  // Generates random single elements FeaturesSubsets
  // def generate: RDD[EvaluatedState[BitSet]] = {

  //   val indexedPopulation = 
  //     (SparkContext.getOrCreate.range(0, populationSize)
  //       .map{ i => 
  //         val fs = FeaturesSubset(BitSet(Random.nextInt(nFeats)), nFeats)
  //         (i, new EvaluatedState[BitSet](fs, evaluator.evaluate(fs)))
  //       }
  //       .partitionBy(partitioner)
  //     )
  //   // Drop indexes (they're only needed for partitioning)
  //   indexedPopulation.map(_._2)
  // }

  // Generates random elements and random size FeaturesSubsets
  // def generate: RDD[EvaluatedState[BitSet]] = {

  //   val indexedPopulation = 
  //     (SparkContext.getOrCreate.range(0, populationSize)
  //       .map{ i => 
  //         val fs = FeaturesSubset(nFeats)
  //         (i, new EvaluatedState[BitSet](fs, evaluator.evaluate(fs)))
  //       }
  //       .partitionBy(partitioner)
  //     )
  //   // Drop indexes (they're only needed for partitioning)
  //   indexedPopulation.map(_._2)
  // }
}

class FeaturesSubsetMutator(var mutationProbability: Float = 0.0F) 
  extends Mutator[BitSet] {

  def mutate(state: EvaluableState[BitSet]): EvaluableState[BitSet] = {

    // Copy the original EvaluableState
    val fs = new FeaturesSubset(
      BitSet.fromBitMaskNoCopy(state.data.toBitMask), 
      state.asInstanceOf[FeaturesSubset].nFeats)

    if (mutationProbability == 0.0F) {
      mutationProbability = 1.0F/fs.nFeats
    }
    for(i <- 0 until fs.nFeats) {
      if(mutationProbability >= Random.nextFloat){
        // TODO test if the are improvements using a mutable BitSet!
        fs.feats = 
          if(fs.feats.contains(i)) {
            fs.feats - i
            // TODO this would prevent empty FeaturesSubset's
            // if (fs.feats.size > 1) (fs.feats - i) else fs.feats
          } else {
            fs.feats + i
          }
      }
    }

    fs
  }
}

class UniformFeaturesSubsetMatchmaker(var swapProbability: Float = 0.0F) extends Matchmaker[BitSet] {

  def crossover(
    stateA: EvaluableState[BitSet],
    stateB: EvaluableState[BitSet]): 
    (EvaluableState[BitSet], EvaluableState[BitSet]) = {

    // Copy original States    
    val fsA = new FeaturesSubset(
      BitSet.fromBitMaskNoCopy(stateA.data.toBitMask), 
      stateA.asInstanceOf[FeaturesSubset].nFeats)
    val fsB = new FeaturesSubset(
      BitSet.fromBitMaskNoCopy(stateB.data.toBitMask), 
      stateB.asInstanceOf[FeaturesSubset].nFeats)

    if (swapProbability == 0.0F) {
      swapProbability = 1.0F/fsA.nFeats
    }
    for(i <- 0 until fsA.nFeats) {
      if(swapProbability >= Random.nextFloat){
        val isInFsA = fsA.feats.contains(i)
        val isInFsB = fsB.feats.contains(i)
        if (isInFsA && !isInFsB) {
          fsA.feats = fsA.feats - i
          fsB.feats = fsB.feats + i
        } else if (isInFsB && !isInFsA){
          fsB.feats = fsB.feats - i
          fsA.feats = fsA.feats + i
        }
      }
    }

    (fsA, fsB)
  }
}