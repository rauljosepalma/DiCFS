#!/bin/bash

#Usage: sh ./checkJobs.sh clustername sessionID

#Use the same name as in the template parameters as argument
clusterName=$1
jobID=$2

#Use the same as in the template
userPass="user:Password"

livyBatch="https://"$clusterName".azurehdinsight.net/livy/batches/$jobID"

curl -k --user $userPass -v -X GET $livyBatch
