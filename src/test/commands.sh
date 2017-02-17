## SPARK ##

spark-submit --master local --class org.apache.spark.ml.feature.Main /home/raul/Desktop/SparkCFS/project-files/target/scala-2.10/spark-cfs_2.10-0.1.0-SNAPSHOT.jar  "/home/raul/Datasets/Toy/iris-discrete.arff" useGA=true useLocallyPred=false &> /home/raul/Desktop/SparkCFS/project-files/src/test/spark.log

## WEKA ##

export CLASSPATH=/home/raul/Software/weka-3-9-0/weka.jar
# Execute with 3800 Mb of heap size
time java -Xmx3800m weka.attributeSelection.ReliefFAttributeEval -M 10 -D 1 -K 10 -i /media/sf_Datasets/Large/ECBDL14/ECBDL14_train_1percent.arff > output.weka.txt &
# -D 1 = Direction Fordward
# -N 5 = Max fails 5
# -L   = Locally predictive false (remove for true)
# -P 1 = Pool size 1
# -E 1 = Threads number 1
time java -Xmx3800m  weka.attributeSelection.CfsSubsetEval -s "weka.attributeSelection.BestFirst -D 1 -N 5 -S 0" -L -P 1 -E 1 -i /home/raul/Datasets/Medium/covtype-discrete.arff &> /home/raul/Desktop/SparkCFS/project-files/src/test/weka.log
time java -Xmx3800m  weka.attributeSelection.CfsSubsetEval -s "weka.attributeSelection.BestFirst -D 1 -N 5 -S 3" -L -P 1 -E 1 -i /home/raul/Datasets/Medium/covtype-discrete.arff &> /home/raul/Desktop/SparkCFS/project-files/src/test/weka.log
