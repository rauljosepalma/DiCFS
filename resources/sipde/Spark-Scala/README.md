##What you can find in this directory?

Please see the README file at each directory for more information.

* config

	In this directory you will find `.properties` files for configuring the SiPDE execution 
	and model definitions and libraries that must be distributed to cluster nodes in some situations
	where they have to be available to SiPDE at execution time.
		
* scripts

	Some scripts to run SiPDE in a cluster or in the AWS and Azure clouds.
		
* src

	The source code of the SiPDE application. The main program is at `src/main/scala/deMain`.
	The implementation of the DE sequential and island-based algorithms is at `src/main/scala/deFunctions`.
	Refer to comments in source files for more information.
	
	Files in `src/main/java` and `src/main/scala/SNA` are part of [SNA](https://code.google.com/archive/p/scala-native-access/).
	
	Files in `src/main/scala/javabbob` are part of the [BBOB](http://coco.gforge.inria.fr) benchmark.

##Using Eclipse

Our project was built following this instructions to setup a maven project for Spark in Eclipse:

### Instructions

[Follow this article to find more detailed instructions.](https://nosqlnocry.wordpress.com/2015/02/27/how-to-build-a-spark-fat-jar-in-scala-and-submit-a-job/)

Modify the class "MainExample.scala" writing your Spark code, then compile the project with the command:

```mvn clean package```

Inside the ```/target``` folder you will find the result fat jar called ```spark-scala-maven-project-0.0.1-SNAPSHOT-jar-with-depencencies.jar```. In order to launch the Spark job use this command in a shell with a configured Spark environment:

    spark-submit --class com.examples.MainExample \
      --master yarn-cluster \
      spark-scala-maven-project-0.0.1-SNAPSHOT-jar-with-depencencies.jar \
      inputhdfspath \
      outputhdfspath

The parameters ```inputhdfspath``` and ```outputhdfspath``` don't have to present the form ```hdfs://path/to/your/file``` but directly ```/path/to/your/files/``` because submitting a job the default file system is HDFS. To retrieve the result locally:

    hadoop fs -getmerge outputhdfspath resultSavedLocally
