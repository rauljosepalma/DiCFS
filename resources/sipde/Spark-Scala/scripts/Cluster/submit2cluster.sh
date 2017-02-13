#!/bin/sh

# set hardware requeriments for qsub command (only needed if qsub is used)
#$ -j y
#$ -cwd
#$ -l s_rt=24:00:00,mem_free=3G,exclusive=true,cpu_type=sandy_bridge
#$ -pe mpi_16p 144

#Path to the SiPDE jar file (modify with your own)
export SPARKJAR=spark-scala-maven-project-0.7-jar-with-dependencies.jar

#Main class name (modify with your own if needed)
export SPARKCLASS=deMain.MainExample

#Properties file (modify it only if a not default location is used)
export PROPFILE=config/sipde.properties

#Uncomment if using MREv (http://mrev.des.udc.es) to setup the Spark Cluster and run SiPDE
#cd ../../MREv/bin
#sh ./run.sh

#Uncomment if using standard spark-submit to run the application on a Spark cluster
$SPARK_HOME/bin/spark-submit --master yarn-client --class $SPARKCLASS $SPARKJAR $PROPFILE
