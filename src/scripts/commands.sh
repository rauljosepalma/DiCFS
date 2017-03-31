## SPARK ##
#=========#

# Local

spark-submit --master local[*] --class org.apache.spark.ml.feature.Main /home/raul/Desktop/SparkCFS/target/scala-2.11/spark-cfs-assembly-0.1.0-SNAPSHOT.jar  /home/raul/Datasets/Large/ECBDL14/head1000-weka-discrete.arff "resultsPath=/home/raul/Desktop/PhD/Papers/DiCFS/results/" sampleSize=1.0 locallyPred=false searchTermination=5 initPartitionSize=5 &> /home/raul/Desktop/SparkCFS/src/test/spark.log

# History
spark-submit --master local[*] --class org.apache.spark.ml.feature.Main --packages sramirez:spark-MDLP-discretization:1.2.1 /home/raul/Desktop/SparkCFS/target/scala-2.10/spark-cfs_2.10-0.1.0-SNAPSHOT.jar  "/home/raul/Datasets/Large/ECBDL14/head1000_train.parquet" &> /home/raul/Desktop/SparkCFS/src/test/spark.log


# Change cores
spark-submit --master local --class sparkfs.Main --total-executor-cores 3 /home/raul/Desktop/Spark-FS/target/scala-2.10/sparkfs_2.10-0.1.0.jar "/home/raul/Datasets/Large/EPSILON/EPSILON.parquet" "/home/raul/Desktop/Spark-FS/papers/DReliefF/results/EPSILON" 10 $i false

# Submit with package
spark-submit --master local --class org.apache.spark.ml.feature.Main --packages "com.databricks:spark-csv_2.10:1.5.0" /home/raul/Desktop/SparkReliefF/target/scala-2.10/spark-relieff_2.10-0.1.0-SNAPSHOT.jar "/home/raul/Desktop/Datasets/Large/ECBDL14/head1000_test.parquet" "/home/raul/Desktop/Datasets/Large/ECBDL14/results/"

# Shell with package
spark-shell --packages "spark-relieff:spark-relieff_2.10:0.1.0-SNAPSHOT" 

# Cluster

/opt/spark/bin/spark-submit --class org.apache.spark.ml.feature.Main /root/cfsPartFeats.jar hdfs://master:8020/datasets/ECBDL14_train-discretized.parquet resultsPath=/root/results/ useLocallyPred=false maxFails=5 partitionSize={0}

/opt/spark/bin/spark-submit --class org.apache.spark.ml.feature.Main /root/cfs.jar hdfs://master:8020/datasets/ECBDL14_train-discretized.parquet useLocallyPred=true useGA=true useNFeatsForPopulationSize=true optIslandPopulationSize=0

/opt/spark/bin/spark-submit --class org.apache.spark.ml.feature.Main /root/relieff.jar hdfs://master:8020/datasets/ECBDL14_${i}percFeats.parquet /root/results/

/opt/spark/bin/spark-submit --class org.apache.spark.ml.feature.Main --packages rauljosepalma:spark-mltools:0.1.0-SNAPSHOT /root/merger.jar  hdfs://master:8020/datasets/ECBDL14_{0}percFeats.parquet


# Update .jar
scp /home/raul/Desktop/SparkCFS/target/scala-2.11/spark-cfs_2.11-0.1.0-SNAPSHOT.jar root@master:/root/cfs.jar
# Update fat .jar
scp /home/raul/Desktop/SparkCFS/target/scala-2.11/spark-cfs-assembly-0.1.0-SNAPSHOT.jar root@master:/root/cfs.jar

# Update cluster-todo.sh
scp /home/raul/Desktop/SparkCFS/src/scripts/cluster-todo.sh root@master:/root/cluster-todo.sh
# Update remote local ivy cache (this only works if Spark is restarted)
scp -r /home/raul/.ivy2/local/*** root@master:/root/.ivy2/local/

# Get results
scp root@master:/root/results/** "/home/raul/Desktop/PhD/Papers/DiCFS/results/"
scp root@master:/root/nohup.out "/home/raul/Desktop/PhD/Papers/DiCFS/results/"

# Execute work
{ rm -f nohup.out ; nohup ./cluster-todo.sh & }
rm -f nohup.out
nohup ./cluster-todo.sh &
# Stop work
ps -ef | grep cluster-todo
kill <pid>

{ sleep 5h ; nohup ./cluster-todo-next.sh ; } &


## WEKA ##
#========#

# Local # 
export CLASSPATH=/home/raul/Software/weka-3-9-1/weka.jar

# ReliefF
# Execute with 3800 Mb of heap size
java -Xmx3800m weka.attributeSelection.ReliefFAttributeEval -M 10 -D 1 -K 10 -i /media/sf_Datasets/Large/ECBDL14/ECBDL14_train_1percent.arff > output.weka.txt &


# Debug #
export CLASSPATH=/home/raul/Software/weka-3-7-12/weka-src/dist/weka.jar
java -Xmx3800m  weka.attributeSelection.CfsSubsetEval -s "weka.attributeSelection.BestFirst -D 1 -N 5" -L -P 1 -E 1 -i /home/raul/Datasets/Large/ECBDL14/head1000-weka-discrete.arff &> /home/raul/Desktop/SparkCFS/src/test/weka.log


# CFS

# -D 1 = Direction Forward
# -N 5 = Max fails 5
# -L   = Locally predictive false (remove for true)
# -P 1 = Pool size 1
# -E 1 = Threads number 1
java -Xmx3800m  weka.attributeSelection.CfsSubsetEval -s "weka.attributeSelection.BestFirst -D 1 -N 5" -L -P 1 -E 1 -i /home/raul/Datasets/Medium/covtype-discrete.arff &> /home/raul/Desktop/SparkCFS/src/test/weka.log
java -Xmx3800m  weka.attributeSelection.CfsSubsetEval -s "weka.attributeSelection.BestFirst -D 1 -N 5" -L -P 1 -E 1 -i /home/raul/Datasets/Medium/covtype-discrete.arff &> /home/raul/Desktop/SparkCFS/src/test/weka.log

# Discretize
# -c last = Use last attribute as class
java -Xmx3800m  weka.filters.supervised.attribute.Discretize -R first-last -precision 6 -c last -i /home/raul/Datasets/Medium/covtype.arff -o /home/raul/Datasets/Medium/covtype-weka-discrete.arff &> /home/raul/Desktop/SparkCFS/src/test/weka.log

# Cluster #
export CLASSPATH=/home/raul/software/weka-3-8-0/weka.jar

# Discretize
{ time java -Xmx120g  weka.filters.supervised.attribute.Discretize -R first-last -precision 6 -c last -i /home/raul/datasets/HIGGS/HIGGS.arff -o /home/raul/datasets/HIGGS/HIGGS_discrete.arff ; } &> /home/raul/results/weka/HIGGS_discreteFay.arff-time.txt &

# CFS
{ time java -Xmx120g  weka.attributeSelection.CfsSubsetEval -s "weka.attributeSelection.BestFirst -D 1 -N 5" -L -P 14 -E 14 -i /home/raul/datasets/HIGGS/HIGGS_discreteFay.arff ; } &> /home/raul/results/weka/HIGGS_discreteFay.arff-time.txt &


## HADOOP & HDFS ##
#=================#

# Start missing datanode:
sh /opt/hadoop/sbin/hadoop-daemon.sh start datanode

# Start missing slave:
# id should be int bigger than the 
sh /opt/spark/sbin/start-slave.sh spark://master:7077

# HDFS Commands
# View dataset sizes:
hdfs dfs -du -h /datasets/
# Remover dir
hdfs dfs -rm -r /datasets/EPSILON.parquet
# Upload from local (master)
bicho$ service smb restart
bicho$ service nmb restart
master$ mount /mnt/shared
master$ hdfs dfs -put /mnt/shared/datasets/data.arff /datasets/

# Decomissioning nodes
# Add node to hadoop/etc/hadoop/dfs.exclude
hdfs dfsadmin -refreshNodes
# Once decomissioned has ended:
ssh hdfs@slave{0} '( /opt/hadoop/sbin/hadoop-daemon.sh stop datanode )'
# Remove them from slaves file