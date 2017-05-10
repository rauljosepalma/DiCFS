package org.apache.spark.ml.feature

class FeaturesSubset(val feats: Seq[Int], domain: Seq[Int] = Seq()) 
  extends EvaluableState {

  // def data: Seq[Int] = feats
  // def data_= (d: Seq[Int]) { feats = d }

  // Returns a sequence of all possible new subsets with one more feat
  override def expand: Seq[EvaluableState] = {
    domain
      .filter { !this.feats.contains(_) } 
      .map { f => new FeaturesSubset(this.feats :+ f, domain) }
  }

  def getPairsWithFeat(feat: Int): Seq[(Int, Int)] = 
    this.feats.zip(Seq.fill(this.feats.size)(feat))

  def getInterFeatPairs: Seq[(Int, Int)] = 
    this.feats.combinations(2).toSeq.map(pair => (pair(0),pair(1)))

  // Descending sort
  def sortedByCorrWithFeat(corrs: CorrelationsMatrix, feat: Int)
    : FeaturesSubset = {
    corrs.precalcCorrs(this.getPairsWithFeat(feat))
    new FeaturesSubset( 
      this.feats.sorted(Ordering.by[Int, Double]{ f => 
      corrs(f, feat) * -1.0
      })
    )   
  }

  def apply(i: Int) = feats(i)

  def ++(fs: FeaturesSubset): FeaturesSubset =
    new FeaturesSubset(this.feats ++ fs.feats)

  def +(i: Int): FeaturesSubset =
    new FeaturesSubset(this.feats :+ i)

  def contains(i: Int) = feats.contains(i)

  def filter(p: (Int) => Boolean): FeaturesSubset =
    new FeaturesSubset(feats.filter(p))

  def map[B](f: (Int) => B): Seq[B] = feats.map(f)
  
  def size: Int = feats.size

  def isEmpty = (this.size == 0)
  // Sequences are immutable
  def toSeq = feats

  override def toString(): String =  feats.mkString(",") 

}

// class EvaluatedFeaturesSubset(val fSubset: FeaturesSubset, merit: Double)
//   extends EvaluatedState[FeaturesSubset](fSubset, merit)