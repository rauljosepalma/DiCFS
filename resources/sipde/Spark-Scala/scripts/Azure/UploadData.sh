#!/bin/bash


#Azure Storage configuration
storageName=<your storage account name>
primaryStorageKey=<your storage account primary key>
secondaryStorageKey=<your storage account secondary key>

#Path to the SiPDE jar file (modify with your own)
sourceJar=spark-scala-maven-project-0.7-jar-with-dependencies.jar

#Remote destination and name where to upload the app jar
containerName=<your BLOB name>
jarName=SB/spark-scala-maven-project-0.7-jar-with-dependencies.jar

#Local path for app configuration to be uploaded (modify with your own)
sourceSB=../../config/Azure/AZURE.zip
#Remote path where to upload app configuration
zipBlob=SB/SB.zip

#Local path of the configuration script
installScript=PrepareCluster.sh
#Remote path where to upload the configuration script
scriptBlob=SB/prepare.sh

#login into azure
azure login -u $username -p $password

#export account name and key
export AZURE_STORAGE_ACCOUNT=$storageName
export AZURE_STORAGE_ACCESS_KEY=$primaryStorageKey

azure storage blob upload $sourceJar $containerName $jarName
azure storage blob upload $sourceSB $containerName $zipBlob
azure storage blob upload $installScript $containerName $scriptBlob
