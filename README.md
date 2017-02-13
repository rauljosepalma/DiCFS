
An Apache Spark based distributed implementation of the classical ReliefF algorithm.

## Original ReliefF

ReliefF is a popular feature weightning algorithm, it was published by Kononenko in 1994 [1] as an extension of the original Relief algorithm developed by Kira and Rendell [2]. ReliefF's central idea consists in evaluating the quality of the features by their ability to distinguish the instances from one class to another in a local neighborhood, i.e. the best features are those contribute more to add distance between different class instances and contribute less to add distance between same class instances. 
After the feature weigthning has been performed, ReliefF can be used as a feature subset selection technique by simply selecting a threshold for choosing the best ranked features. 

## Distributed ReliefF

In this repository we implement a distributed version of ReliefF based on the Apache Spark model. Even when the ReliefF algorithm can be considered embarrasingly parallel because its main loop can be separated into independent tasks and then the results can be merged, this implementation does not follow this design because it leads to a tying of the number of samples used by the algorithm with the number of independent tasks that can be run. In addition, this design also leads to having each independent task reading the whole dataset. In short, the actual implementation has the following features:

* After the max and min values of every feature and the prior probabilites are know, the algorithm only requires two passes through the whole dataset.
* The number of tasks is not tied to the number of samples, instead the number of tasks is fully determined by Spark, considering the available resoures and the size of the dataset.
* The algorithm has been tested to be scalable and with better memory and execution time consumption.

LAST_LINE

## License

## References

