import org.apache.spark.ml.feature
import scala.collection.immutable.BitSet

// Mutator
println("Testing mutation")
val fs = feature.FeaturesSubset(10)
println("fs.genotype = " + fs.genotype.toString)
var mutatedFs: feature.Individual[BitSet] = _
var i = 0
do {
  mutatedFs = (new feature.FeaturesSubsetMutator).mutate(fs)
  i += 1
} while (mutatedFs.genotype == fs.genotype)
println("mutatedFs.genotype = " + mutatedFs.genotype.toString)
println(s"number of test before mutation = $i")

// Matchmaker

val fsA = feature.FeaturesSubset(10)
fsA.genotype
val fsB = feature.FeaturesSubset(10)
fsB.genotype
i = 0
var crossoveredFs: (feature.Individual[BitSet],feature.Individual[BitSet]) = _
do {
  crossoveredFs = 
    (new feature.UniformFeaturesSubsetMatchmaker).crossover(fsA, fsB)
  i += 1
} while (fsA.genotype == fsA2.genotype && fsB.genotype == fsB2.genotype)
crossoveredFs._1.genotype
crossoveredFs._2.genotype
println(s"number of test before crossover = $i")
