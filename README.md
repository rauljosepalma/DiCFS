An Apache Spark based distributed implementation of the classical CFS algorithm. Current implementation uses Spark 2.1.0 and the DataFrame-based API.
A branch with support for Spark 1.6.1 is also available, however it also uses the DataFrame-based API.

## Original CFS

CFS (Correlation Based Feature Selection) is one of the most important Feature Selection algorithms successfully applied in many domains, it was published by Hall in 2000 [1]. For the case of classification CFS requires a discretized dataset and uses an information theory based heuristic for the selection of the best subset of features.

## Distributed CFS

In this repository we implement a distributed version of CFS based on the Apache Spark model. The version has been tested to be much more time efficient and scalable than the original version in WEKA.

This repository is associated to a paper already submitted for publication.

## References

[1] Hall, M. A. (2000). Correlation-based Feature Selection for Discrete and Numeric Class Machine Learning, 359–366.

## Example of Use

```scala
import org.apache.spark.ml.feature.CFSSelector
...

val df = //Read a DataFrame

val fSelector = { new CFSSelector()
  .setFeaturesCol("features")
  .setLabelCol("label")
  .setOutputCol("prediction")
  .setLocallyPredictive(true)
  .setSearchTermination(5)
  .setVerticalPartitioning(true))
  .setNPartitions(0)
}

val model = fSelector.fit(df)
val selectedFeats: Array[Int] = model.selectedFeats
```