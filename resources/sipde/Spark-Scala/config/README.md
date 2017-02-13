In this directory you will find `.properties` files for configuring the SiPDE execution 
and model definitions and libraries that must be distributed to cluster nodes in some configurations.

*********************************************************************************************
Please refer to `../scripts/README` for more information on how to use these files.	
*********************************************************************************************

## Contents

#### template.properties

A `.properties` template that can be used as reference. Refer to comments in the template file 
for more information about execution parameters and their values.

#### LICENSE\_BBOB.txt and LICENSE\_HDF5.txt

License files of libraries distributed with SiPDE. Refer to the Libraries section 
below for more information.

#### Cluster, AWS and Azure directories

Files to be distributed to cluster nodes. They have been grouped into these three directories for
convenience, corresponding to the three different execution platforms in which SiPDE has
been tested.

There are small differences in the placement of files among platforms. It is __VERY IMPORTANT__ to 
distribute the files in the same relative locations as they are provided (e.g. for clusters, SiPDE
expects the files to be placed into an SB directory).

In the following the files contained in those directories are described:

#### Properties file (sipde.properties)

The file used to configure the SiPDE execution. 
It can be found in the properties subdirectory of each execution platform (exception made for AWS).
Refer to comments in the file for more information about execution parameters and their values. 

#### Model definitions

Three different Biological System models (Circadian, Nfkb and Mendes) are distributed with SiPDE.
They can be found in the config subdirectory of each execution platform.

These models have been used in our experiments through the AMIGO toolbox. The toolbox is accessed
from SiPDE using an interface to an external C library (see more on this below). 

#### Libraries
	
In some configurations SiPDE has runtime dependencies with some external libraries. They 
can be found in the lib subdirectory of each execution platform (exception made for Azure). These 
libraries are the following:

__libcjavabbob.so__

This library contains the BBOB benchmarks distributed as part of the COCO software.
It must be provided to cluster nodes when using the BBOB benchmarking functions.
For more information on COCO see: [http://coco.gforge.inria.fr](http://coco.gforge.inria.fr).
	
__libSBLib.so__

This library contains both all that is needed to run Biological System models 
with the AMIGO toolbox and the implementation of some local solvers. 
It must be provided to cluster nodes both when using the Biological System models 
(i.e. Circadian, Nfkb or Mendes) or when using the NL2SOL solver.
For more information on AMIGO see: [http://gingproc.iim.csic.es/~amigo/index.html](http://gingproc.iim.csic.es/~amigo/index.html)

The library was compiled from source code provided by the (Bio)Process Engineering 
Group at IIM-CSIC (Vigo, Spain, [http://gingproc.iim.csic.es/](http://gingproc.iim.csic.es/)) 
that is the holder of the copyright. The source code is still
under development and currently is not publicy available. The intention is to distribute
it to the general public under a free license in the near future. In the meanwhile
permission is granted to use the library as it is with the only purpose of testing 
the SiPDE program with the aforementioned Biological System models and solver.
	
__libhdf5.so.8, libhdf5_fortran.so.8__

libSBLib.so has a dependency on the HDF5 libraries. They must be also distributed to cluster 
nodes when using the Biological System models or the NL2SOL solver. They are
included here only for convenience because chances are they are already installed in
your system.

__GNU Scientific Library__

This library is not distributed with SiPDE but it is needed to use the Biological Systems
models or the NL2SOL solver. Its location must be added to the library path of each cluster node.

	
*********************************************************************************************

Note that these libraries are not needed if you provide your own custom benchmarking function 
or use one of our own implementations when running SiPDE. See the file `template.properties`
for more details.

*********************************************************************************************

#### Compressed .zip file

Contents of each execution platform directory has to be zipped into a file before being distributed 
to cluster nodes. An example .zip file is included into each execution platform directory for
convenience.