This directory contains sample scripts and template files to run SiPDE in a cluster or in AWS and Azure clouds. 

#How to run the application?

_Note_: in the following explanations it is assumed that you have compiled SiPDE into a jar file.


## __In a cluster__

##### Requeriments

A Spark distribution must be installed and configured in the cluster.

_Note_: if you don´t still have Spark installed, consider using a tool like [MREv](http://mrev.des.udc.es) 
to automatically setup a Spark cluster and run SiPDE.

##### Previous steps

1. Zip the contents of the `../config/Cluster/` directory (or use the provided Cluster.zip 
file in that directory), copy the zip file to the cluster and unzip it at your $HOME. You 
will get a copy of `../config/Cluster/` at `$HOME/SB/` in your cluster.

2. Copy the `./Cluster/submit2cluster.sh` script to `$HOME/SB/` (you also can include it in
the zip file if you prefer). 

3. Configure your cluster to make `$HOME/SB/` accessible to all cluster nodes where SiPDE
is going to be executed. Add `$HOME/SB/lib` to the library path of each node.

_Note_: if you are going to use the Biological Systems models or the NL2SOL solver, the GNU 
Scientific Library (libgsl) must also be installed in your cluster and in the library path
of each node.

##### Running SiPDE

4. Edit the file `$HOME/SB/properties/sipde.properties` to configure the SiPDE execution.

5. Edit the `$HOME/SB/submit2cluster.sh` script to your needs and execute it. 

_Note_: redirect the standard output to a file to store it.
 

## __In the AWS cloud__

##### Requeriments

* You must have an AWS account and an IAM access key downloaded to your computer.
* You must have a local Spark installation. This is needed to have the 
[spark-ec2](https://spark.apache.org/docs/latest/ec2-scripts.html) script installed
in your computer. 

##### Previous steps

1. Edit the `./AWS/awscmd.sh` to your needs. This script provides several commands to 
interface with AWS. Refer to comments in the script for more information.

2. Use the `awscmd.sh create` command to start a new virtual cluster in AWS.

3. Edit the `./AWS/submit2aws.sh` script to indicate the Spark master URI in the virtual
cluster you have started.

4. Zip the contents of the `../config/AWS/` directory (or use the provided AWS.zip 
file in that directory), upload the zip file to the virtual cluster and unzip it at 
`/root/SparkFolder/` (create it if needed). You will get a copy of `../config/AWS/` at
`/root/SparkFolder/` in your virtual cluster.

##### Running SiPDE

5. Edit a properties file to configure the SiPDE execution. You can create a new one
copying `../config/template.properties`. The location of the properties file has to be 
properly configured in `./AWS/awscmd.sh`

6. Use the `awscmd.sh launch` command to deploy the SiPDE jar, the properties file and the Spark
configuration files in `./AWS` to the virtual cluster and to run SiPDE. Alternatively 
you can do it in two steps (deploy + run commands).

7. Use the `awscmd.sh getOutput` command to download the execution output to a local file.
	
__IMPORTANT:__ DON'T USE the `./AWS/submit2aws.sh` script directly. All the files in 
the `./AWS` directory are automatically uploaded and used by the `awscmd.sh` script to 
configure Spark in the remote virtual cluster. Edit them only if you need to change
the Spark configuration.


## __In the Azure cloud__

##### Requeriments

* You must have an Azure account.
* You must setup a BLOB container in Azure storage and get its access keys (refer
to Azure HDInsight documentation for more information).
* You must locally install the Azure CLI (Command Line Interface).

##### Previous steps

1. Prepare config files

	* Edit the file `../config/Azure/properties/sipde.properties` to configure the SiPDE execution.

	* Zip the contents of the `../config/Azure/` directory into a file called `AZURE.zip`.

	* Edit the `./Azure/PrepareCluster.sh` script to your needs.

2. Upload files to Azure BLOB storage. Edit the `./Azure/UploadData.sh` script to your needs and execute. 
It will upload the files needed to configure the HDInsight cluster and execute SiPDE to your Azure BLOB container.

3. Start the HDINSIGHT cluster. Edit the `./Azure/startCluster.sh` script to your needs and execute. This script 
will start an HDINSIGHT cluster using template and parameter files. Default JSON template 
(`./Azure/template.json`) and parameter (`./Azure/6worker.json`) files are provided for 
convenience. Edit them to your needs or provide your own files (refer to the Azure HDInsight 
documentation for more information).

##### Running SiPDE

4. Submit the application. Edit the `./Azure/runJob.sh` script to your needs and execute.

5. Check for application state. Edit the `./Azure/checkJobs.sh` script to your needs and execute.

6. Get the results. Currently we have no script for that. You have to download it manually login into your
Azure account and using the cluster YARN interface to locate the job output.

7. Terminate the HDINSIGHT cluster. Currently we have no script for that. Once you have finished, terminate 
the HDINSIGHT cluster manually.

_Note_: you can also kill the execution of a runinng SiPDE with the `./Azure/DeleteJobs.sh` script.






	