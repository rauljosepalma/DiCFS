package org.apache.spark.ml.feature

import scala.collection.immutable.BitSet

class FeaturesSubset(feats: BitSet, domain: BitSet) 
  extends EvaluableState[BitSet] {

  def data: BitSet = feats
  // def data_= (d: BitSet) { feats = d }
  def size: Int = feats.size

  // Returns a sequence of all possible new subsets with one more feat
  def expand: Seq[EvaluableState[BitSet]] = {
    domain
      .filter { !this.feats.contains(_) } 
      .map { f => new FeaturesSubset(this.feats + f, domain) }
      .toSeq
  }

}