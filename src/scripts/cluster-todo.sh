/opt/spark/bin/spark-submit --class org.apache.spark.ml.feature.Main /root/cfs.jar hdfs://master:8020/datasets/ECBDL14_train-discretized.parquet useLocallyPred=true useGA=true useNFeatsForPopulationSize=true optIslandPopulationSize=0

# History
# /opt/spark/bin/spark-submit --class org.apache.spark.ml.feature.Main /root/cfs.jar  hdfs://master:8020/datasets/ECBDL14_traindiscretized0-629.parquet hdfs://master:8020/datasets/ECBDL14_train-discretized-columns.parquet hdfs://master:8020/datasets/ECBDL14_train-discretized.parquet
# /opt/spark/bin/spark-submit --class org.apache.spark.ml.feature.Main /root/QuantileDiscretizer.jar  hdfs://master:8020/datasets/ECBDL14_train.arff