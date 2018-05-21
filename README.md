
An Apache Spark based distributed implementation of the classical CFS algorithm.

## Original CFS

CFS (Correlation Based Feature Selection) is one of the most important Feature Selection algorithms successfully applied in many domains, it was published by Hall in 2000 [1]. For the case of classification CFS requires a discretized dataset and uses an information theory based heuristic for the selection of the best subset of features.

## Distributed CFS

In this repository we implement a distributed version of CFS based on the Apache Spark model. The version has been tested to be much more time efficient and scalable than the original version in WEKA.

This repository is associated to a paper already submitted for publication.

## References

[1] Hall, M. A. (2000). Correlation-based Feature Selection for Discrete and Numeric Class Machine Learning, 359–366.

