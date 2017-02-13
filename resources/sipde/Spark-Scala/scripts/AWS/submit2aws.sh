#!/bin/bash

#Spark master node URI
#Note: need to be changed every time a cluster is started if not using an elastic IP
MASTER=spark://

JAR=/root/SparkFolder/scala.jar
CONFIG=/root/SparkFolder/sipde.properties

MAINCLASS=deMain.MainExample

#rsync directory to slaves
~/spark-ec2/copy-dir ./SparkFolder
#rsync configuration folder to slaves
~/spark-ec2/copy-dir ./spark/conf

#launch Spark app
echo "Running Application"
cd /root/SparkFolder/
/root/spark/bin/spark-submit --class $MAINCLASS --master $MASTER $JAR $CONFIG > /root/SparkFolder/output.txt
#./SparkFolder/salida.txt > ./SparkFolder/output.txt


