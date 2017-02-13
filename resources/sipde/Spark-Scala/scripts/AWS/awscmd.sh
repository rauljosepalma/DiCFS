#!/bin/bash
#
# This script includes the following commands:
#
#	create, destroy - create/destroy a virtual cluster in AWS
#	start, stop - start/stop the virtual cluster
#	login - login into the frontend node
#
#	deploy - upload application jar and configuration files to the frontend 
#	run - run the uploaded application
#	launch - deploy + run
#
#	getOutput - download the output from the cluster to a local file
#

#Spark EC2 binaries path
BINLOC=$SPARK_HOME/ec2
#Number of nodes (without counting the frontend)
SLAVES=16
#Cluster name
CNAME=16Nodes
#AWS Access Key
KEY=
KEYFILE=
#Instance type
ITYPE=m3.medium
#AWS region
REGION=us-west-2
#Master node (frontend) IP
#Note: need to be changed every time a cluster 
#is started if not using an elastic IP
MASTERIP=

#Path to the SiPDE jar file (modify with your own)
JARFILE=spark-scala-maven-project-0.7-jar-with-dependencies.jar
#Properties file (modify it only if a not default location is used)
CONFIGFILE=config/sipde.properties
#Spark configuration files (default ones are provided for convenience)
SPARKCONFFILE=./spark-defaults.conf
LOGFILE=./log4j.properties
#Local output file location (modify with your own)
LOCALOUT=$HOME/SparkOut.txt
#Destination in the frontend for jar and configuration files
DESTJAR=/root/SparkFolder/scala.jar
DESTCONFIG=/root/SparkFolder/sipde.properties
DESTLOG=/root/spark/conf/log4j.properties
DESTSPARKCONF=/root/spark/conf/spark-defaults.conf
CLUSTEROUT=/root/SparkFolder/output.txt

echo $1

if [ $1 = "create" ]; then
	$BINLOC/spark-ec2 -k $KEY -i $KEYFILE -s $SLAVES --copy-aws-credentials --instance-type=$ITYPE --region=$REGION launch $CNAME
fi

if [ $1 = "start" ]; then
	$BINLOC/spark-ec2 -i $KEYFILE --region=$REGION start $CNAME
fi

if [ $1 = "stop" ]; then
	$BINLOC/spark-ec2 --region=$REGION stop $CNAME
fi

if [ $1 = "destroy" ]; then
	$BINLOC/spark-ec2 --region=$REGION destroy $CNAME
fi

if [ $1 = "login" ]; then
	$BINLOC/spark-ec2 -k $KEY -i $KEYFILE --region=$REGION login $CNAME
fi


#complete proccess, send executables, run app and bring back results	
if [ $1 = "launch" ]; then
	#send jar and config file to cluster
	ssh -i $KEYFILE root@$MASTERIP "mkdir SparkFolder"
	ssh -i $KEYFILE root@$MASTERIP "mkdir SparkFolder/libs"
	scp -i $KEYFILE $JARFILE root@$MASTERIP:$DESTJAR
	scp -i $KEYFILE $CONFIGFILE root@$MASTERIP:$DESTCONFIG
	
	#send log4j config file, to log errors to file
	scp -i $KEYFILE $LOGFILE root@$MASTERIP:$DESTLOG
	#send spark-default.conf, to set Spark configuration file
	scp -i $KEYFILE $SPARKCONFFILE root@$MASTERIP:$DESTSPARKCONF
	
	#launch execution
	ssh -i $KEYFILE root@$MASTERIP "bash -s" < ./submit2aws.sh
	
	#bring output back from cluster
	scp -i $KEYFILE root@$MASTERIP:$CLUSTEROUT $LOCALOUT
	
fi

#Only send the executables
if [ $1 = "deploy" ]; then
	#make dir on remote machine (fails if exists)
	ssh -i $KEYFILE root@$MASTERIP "mkdir SparkFolder"
	#send jar and config file to cluster
	#rsync -aRvuze 'ssh -i '$KEYFILE  $JARFILE root@$MASTERIP:$DESTJAR
	scp -i $KEYFILE $JARFILE root@$MASTERIP:$DESTJAR
	#rsync -aRvuze 'ssh -i '$KEYFILE $CONFIGFILE root@$MASTERIP:$DESTCONFIG
	scp -i $KEYFILE $CONFIGFILE root@$MASTERIP:$DESTCONFIG
	
	#send log4j config file, to log errors to file
	scp -i $KEYFILE $LOGFILE root@$MASTERIP:$DESTLOG
	#send spark-default.conf, to set Spark configuration file
	scp -i $KEYFILE $SPARKCONFFILE root@$MASTERIP:$DESTSPARKCONF
fi

#run Spark App (assumes application is already deployed)
#log in master, rsyncs directory and run App
if [ $1 = "run" ]; then
	ssh -i $KEYFILE root@$MASTERIP "bash -s" < ./submit2aws.sh
	scp -i $KEYFILE root@$MASTERIP:$CLUSTEROUT $LOCALOUT
fi

if [ $1 = "getOutput" ]; then
	scp -i $KEYFILE root@$MASTERIP:$CLUSTEROUT $LOCALOUT
fi

