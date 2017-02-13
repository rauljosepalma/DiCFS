#!/bin/bash

deploymentName="SparkDE6N"
resourceGroupName="your resource group"
#Cluster template (use your own or modify the default provided one)
templateFilePath=./template.json
#Template parameters (provided a cluster with 6 nodes)
parametersFilePath=./6worker.json

#Azure Storage key
primaryStorageKey=
secondaryStorageKey=

#login to azure using your credentials
azure login

#switch the mode to azure resource manager
azure config mode arm

#Start deployment
echo "Starting deployment..."
azure group deployment create --name $deploymentName --resource-group $resourceGroupName --template-file $templateFilePath --parameters-file $parametersFilePath
