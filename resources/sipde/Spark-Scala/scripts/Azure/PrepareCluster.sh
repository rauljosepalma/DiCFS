#!/bin/bash

#This scripts should be executed in all cluster nodes, to ensure all the nodes
#have the needed files.
#Needs the following environment variables being defined: storageName, primaryStorageKey

SBfolder=/opt/SB
containerName=<your BLOB name>

#Location of the app jar in Azure Storage account 
jarBlob="wasb://yourcluster@yourstorage.blob.core.windows.net/SB/spark-scala-maven-project-0.7-jar-with-dependencies.jar"
#Destination of the app jar in the nodes file system 
jarLocation=$SBfolder/spark-scala-maven-project-0.7-jar-with-dependencies.jar

#Location of the app configuration files in Azure Storage account 
zipBlob="wasb://decluster@eclibstorage.blob.core.windows.net/SB/SB.zip"
#Destination of the app configuration files in the nodes file system 
zipLocation=$SBfolder/SB.zip

export AZURE_STORAGE_ACCOUNT=$storageName
export AZURE_STORAGE_ACCESS_KEY=$primaryStorageKey

#Create directories for data, on /opt
mkdir $SBfolder

#download jar blob
rm $jarLocation
hadoop fs -copyToLocal $jarBlob $jarLocation 
#download SB zip file (library binary and config folder, and properties files)
rm $zipLocation
hadoop fs -copyToLocal $zipBlob $zipLocation 
#unzip SB zip file
cd $SBfolder
unzip -o $zipLocation 

#add read and run permissions to everyone
chmod -R 755 $SBfolder

#make this directory available for libraries, with LD_LIBRARY_PATH
echo "LD_LIBRARY_PATH=/opt/SB:$LD_LIBRARY_PATH" | tee -a /etc/environment

#Install libSBLib dependencies: libgsl0-dev
sudo apt-get install -y libgsl0-dev libgfortran-4.8-dev
