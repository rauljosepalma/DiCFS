package org.apache.spark.ml.feature

import scala.util.Random

class FeaturesSubset(val feats: Seq[Int], domain: Seq[Int] = Seq()) 
  extends EvaluableState {

  require(feats.distinct.size == feats.size, 
    "A FeatureSubset cannot have repeated elements")

  // Returns a sequence of all possible new subsets with one more feat
  override def expand: Seq[FeaturesSubset] = {
    domain
      .filter { !this.feats.contains(_) } 
      .map { f => new FeaturesSubset(this.feats :+ f, domain) }
  }

  // iClass will always be greater than all feats
  def getPairsWithClass(iClass: Int): Seq[(Int, Int)] = 
    this.feats.zip(Seq.fill(this.feats.size)(iClass))

  // Sorted makes sure that the resulting meets (i,j): i < j
  def getInterFeatPairs: Seq[(Int, Int)] = 
    this.feats.sorted.combinations(2).toSeq.map(pair => (pair(0),pair(1)))

  // Descending sort
  def sortedByCorrWithClass(corrs: CorrelationsMatrix, iClass: Int)
    : FeaturesSubset = {
    corrs.precalcCorrs(this.getPairsWithClass(iClass))
    new FeaturesSubset( 
      this.feats.sorted(Ordering.by[Int, Double]{ f => 
      corrs(f, iClass) * -1.0
      })
    )   
  }

  def sortedRandom = new FeaturesSubset(Random.shuffle(this.feats))

  def apply(i: Int) = feats(i)

  def ++(fs: FeaturesSubset): FeaturesSubset =
    new FeaturesSubset((this.feats ++ fs.feats).distinct)

  def +(i: Int): FeaturesSubset =
    if (!this.contains(i))
      new FeaturesSubset(this.feats :+ i)
    else 
      this

  def contains(i: Int) = feats.contains(i)

  def filter(p: (Int) => Boolean): FeaturesSubset =
    new FeaturesSubset(feats.filter(p))

  def map[B](f: (Int) => B): Seq[B] = feats.map(f)
  
  def size: Int = feats.size

  def isEmpty = (this.size == 0)
  // Sequences are immutable
  def toSeq = feats

  override def toString(): String =  feats.sorted.mkString(",") 

}

// class EvaluatedFeaturesSubset(val fSubset: FeaturesSubset, merit: Double)
//   extends EvaluatedState[FeaturesSubset](fSubset, merit)