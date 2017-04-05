# DICED (Diced Is Cloud-Enabled DVID) [![Picture](https://raw.github.com/janelia-flyem/janelia-flyem.github.com/master/images/HHMI_Janelia_Color_Alternate_180x40.png)](http://www.janelia.org)

This package enables storage and access of massive data arrays both locally and
on the cloud.  It incorporates ideas from github, like versioning and notions
of different repositories of data, to allow effective management of large array
data.  One notable application of DICED is to store terascale 3D image data
as this package will chunk the data to enable fast random access.

nD array data is often manipulated using numpy arrays in python
and persisted on disk using file formats like hd5.  DICED provides
easy python access to arrays like numpy but it also allows for handling
of arbitrarily large (up to signed 32-bit integer) for each dimension
and exploits modern large-scale distributed storage on the cloud.
In this way, it is possible to use DICED for massive, high-throughput reads,
without requiring an expensive disk solution to improve throughput
to a single hdf file. 

## Installation

The primary dependencies are:

* [DVID](https://github.com/janelia-flyem/dvid.git)
* [libdvid](https://github.com/janelia-flyem/libdvid-cpp.git)

The preferred installation method is the conda build system, which
installs all dependencies and DICED.  It is possible to manually
install the dependencies defined in the conda-recipe folder and
run "python setup.py install".

### conda installation

The [Miniconda](http://conda.pydata.org/miniconda.html) tool first needs to installed:

```
# Install miniconda to the prefix of your choice, e.g. /my/miniconda

# LINUX:
wget https://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh
bash Miniconda-latest-Linux-x86_64.sh

# MAC:
wget https://repo.continuum.io/miniconda/Miniconda-latest-MacOSX-x86_64.sh
bash Miniconda-latest-MacOSX-x86_64.sh

# Activate conda
CONDA_ROOT=`conda info --root`
source ${CONDA_ROOT}/bin/activate root
```
Once conda is in your system path, call the following to install libdvid-cpp:

    % conda create -n <NAME> -c flyem diced

Conda allows builder to create multiple environments.  To use the python
library, set your PATH to the location of PREFIX/envs/< NAME >/bin. 

### Developers

Install DICED with conda.  Subsequent changes to the DICED source can
be rebuilt against the conda installation by 'python setup.py install'.


## Tutorial and Examples
TBD


## Currently Limitations/Future Work

* Only 1, 2, and 3D data is currently supported (eventual support for >3D)
* Data fetching can be done in parallel but currently writing can
only be done by one writer unless special care is taken (see **Performance Considerations** below).
* Google Cloud Storage is the only cloud store supported at this time 

