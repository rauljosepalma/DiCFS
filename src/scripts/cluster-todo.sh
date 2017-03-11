/opt/spark/bin/spark-submit --class org.apache.spark.ml.feature.Main /root/cfs.jar hdfs://master:8020/datasets/HIGGS_discreteFay.parquet useLocallyPred=true useGA=false useNFeatsForPopulationSize=false optIslandPopulationSize=0

java -Xmx3800m  weka.attributeSelection.CfsSubsetEval -s "weka.attributeSelection.BestFirst -D 1 -N 5 -S 0" -L -P 1 -E 1 -i /home/raul/Datasets/Medium/covtype-discrete.arff &> /home/raul/Desktop/SparkCFS/project-files/src/test/weka.log


# History
# /opt/spark/bin/spark-submit --class rauljosepalma.sparkmltools.Main /root/tmp.jar hdfs://master:8020/datasets/HIGGS.arff
# /opt/spark/bin/spark-submit --class org.apache.spark.ml.feature.Main /root/cfs.jar hdfs://master:8020/datasets/ECBDL14_train-discretized_{0}perc.parquet useLocallyPred=true useGA=true useNFeatsForPopulationSize=true optIslandPopulationSize=0
# /opt/spark/bin/spark-submit --class org.apache.spark.ml.feature.Main /root/cfs.jar  hdfs://master:8020/datasets/ECBDL14_traindiscretized0-629.parquet hdfs://master:8020/datasets/ECBDL14_train-discretized-columns.parquet hdfs://master:8020/datasets/ECBDL14_train-discretized.parquet
# /opt/spark/bin/spark-submit --class org.apache.spark.ml.feature.Main /root/QuantileDiscretizer.jar  hdfs://master:8020/datasets/ECBDL14_train.arff