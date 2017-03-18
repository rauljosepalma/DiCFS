package org.apache.spark.ml.feature

class FeaturesSubset(feats: Seq[Int], domain: Seq[Int]) 
  extends EvaluableState[Seq[Int]] {

  def data: Seq[Int] = feats
  // def data_= (d: Seq[Int]) { feats = d }
  def size: Int = feats.size

  // Returns a sequence of all possible new subsets with one more feat
  def expand: Seq[EvaluableState[Seq[Int]]] = {
    domain
      .filter { !this.feats.contains(_) } 
      // +: prepend is O(c) on Lists, append is O(L)
      .map { f => new FeaturesSubset(f +: this.feats, domain) }
  }

}