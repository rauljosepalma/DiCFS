spark-submit --master local --class org.apache.spark.ml.feature.Main /home/raul/Desktop/SparkCFS/project-files/target/scala-2.10/spark-cfs_2.10-0.1.0-SNAPSHOT.jar  "/home/raul/Datasets/Large/ECBDL14/head1000-discretized.arff" "BESTFIRST"

spark-submit --master local --class org.apache.spark.ml.feature.Main /home/raul/Desktop/SparkCFS/project-files/target/scala-2.10/spark-cfs_2.10-0.1.0-SNAPSHOT.jar  "/home/raul/Datasets/Large/ECBDL14/head1000-discretized.arff" "GASEARCH"
