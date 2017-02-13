#SiPDE

##A Spark implementation of the island-based parallel Differential Evolution

This is the first release of the implementation of an island-based model for the 
[Differential Evolution](https://en.wikipedia.org/wiki/Differential_evolution) 
algorithm presented for the first time at [EvoPAR2016](http://www.evostar.org/2016/cfp_evopar.php).
It has been coded in [Scala](http://www.scala-lang.org) and uses [Spark](https://spark.apache.org) for being
executed on commodity clusters or Cloud resources.

###Main features

The following is included in this release:

* Two implementations for the Differential Evolution in Scala: the sequential and the island-based model. The island-based implementation includes two enhancements to what has been presented at EvoPar:
	1. An optional asynchronous Local Solver (NL2SOL solver is used).
	2. Support for heterogeneous island configurations.

    These enhancements are documented in this [technical report](https://bitbucket.org/xcpardo/sipde/raw/58d12524bb9372f3f5a35f8f4f0d6027bc76007f/techreport2016.pdf) that currently is under review after being
submitted for publication.

* Support for different benchmarking functions:

	1. Our own implementations for the Sphere, Schwefel, Rosenbrock, Rastrigin, Ackley and Griewank functions
	2. Functions from the [BBOB](http://coco.gforge.inria.fr) benchmark: Rosenbrock (f8), Rastrigin (f15), 
	Schaffers (f17), GriewankRosenbrock (f19), Schwefel (f20) and Gallagher (f22)
	3. Biological Systems models: Circadian, Mendes, Nfkb
	4. Your own custom function
	
* All that is necessary to run the program in a cluster or in the AWS and Azure clouds using Spark.


###Documentation

  Please see the README file at each directory and comments in the source files for more information.

###Licensing

  Please see the file called LICENSE.

###Referencing

All publications mentioning features or use of this software are asked to credit the authors by
citing the following references:

* Diego Teijeiro, Xoán C. Pardo, Patricia González, Julio R. Banga, Ramón Doallo: Implementing 
Parallel Differential Evolution on Spark. EvoApplications (2) 2016: 75-90
[DBLP](http://dblp.org/rec/conf/evoW/TeijeiroPGBD16)
[DOI](http://dx.doi.org/10.1007/978-3-319-31153-1_6)

###Known issues

Original BBOB (`libcjavabbob.so`) and Biological Systems (`libSBLib.so`) libraries are not thread safe. Although we 
provide a thread-safe interface to these libraries, it is a very early version not completely bug-free. To avoid
bad results or even application crashes use them with Spark configurations containing single-threaded executors only. 

###Contact

This work is the result of a collaboration between the Computer Architecture Group ([GAC](http://gac.udc.es/english/))
at Universidade da Coruña ([UDC](http://www.udc.gal/index.html?language=en)) and the (Bio)Processing Engineering Group 
([GIngProg](http://gingproc.iim.csic.es)) at [IIM-CSIC](http://www.iim.csic.es).

* Contacts at GAC

	Xoan C. Pardo <[xoan.pardo@udc.gal](mailto:xoan.pardo@udc.gal)>
	Patricia Gonzalez <[patricia.gonzalez@udc.es](mailto:patricia.gonzalez@udc.es)>
   
* Contact at IIM-CSIC

	Julio R. Banga <[julio@iim.csic.es](mailto:julio@iim.csic.es)>