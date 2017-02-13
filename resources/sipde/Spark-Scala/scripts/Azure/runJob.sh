#!/bin/bash

#Usage: sh ./runJob.sh ClusterName

#Use the same name as in the template parameters as argument
clusterName=$1
#Use the same as in the template
userPass="user:Password"


livyBatch="https://"$clusterName".azurehdinsight.net/livy/batches"

#Location of app jar, main class name and configuration file
jarFile=wasb://yourcluster@yourstorage.blob.core.windows.net/SB/spark-scala-maven-project-0.7-jar-with-dependencies.jar
className=deMain.MainExample
propFile=/opt/SB/properties/sipde.properties

#submit job to the cluster
curl -k --user $userPass -v -H 'Content-Type: application/json' -X POST -d '{ "file":"'$jarFile'", "className":"'$className'", "args": ["'$propFile'"] }' $livyBatch
