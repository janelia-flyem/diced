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

### General workflow

* Create a DicedStore which spawns a DVID instance for access versioned array data.
* Create or open a DicedRepo from the DicedStore.  This is similar to opening or creating a repository in Github.
* Create nD DicedArray types in the repo
* Visualize repo information on the html web viewer that is launched on the same port as the DicedStore (default port 8000)
* Lock, branch version nodes as your data repository changes.

### Scenario 1: Create a local database to store image data

    % from diced import DicedStore, ArrayDtype
    % store = DicedStore("~/myrepo.diced") # location of diced repo
    % repo = store.create_repo("firstrepo", "My first repo")
    % arr = repo.create_array("arrayname", ArrayDtype.uint16) # default 3D array
    % arr[0:1, 0:1, 0:5] = numpy.array([[[3,2,4,1,6]]])
    % val = arr[0,0,0] # val = 3

By default this will start a DicedStore that runs on 127.0.0.1:8000 with
a corresponding web interface.  The repo is created with a unique id (UUID).

### Scenario 2: Access Google storage

* Create a Google cloud account.
* Create a blank google storage bucket (e.g., mybucketname)
* Download permissions to access the bucket as a JSON

Accessing the data is the same as the first example but the creation
of the DicedStore is different.

    % DiceStore("mybucketname", permissionfile="path/to/my/key.json"

### Scenario 3: Versioning Data

For the above examples, one can lock this version and create a new
version of the array data.  This has conceptual similarities to Git
but unlike Git the unique identified (UUID) used by DICED is determined
before writing data for a given version node.  In other words,
a unique identifier is not a content-based hash and is set up front.

    % repo.lock_node("finished work") # can no longer write to data
    % repo.create_branch("new branch from locked node") # can only branch from locked node
    % arr[0:1, 0:1, 0:5] = numpy.array([[[4,2,4,1,6]]])
    % val = arr[0,0,0] # val = 4 only in this version 

## Current Limitations/Future Work

* Only 1, 2, and 3D data is currently supported (eventual support for >3D)
* Data fetching can be done in parallel but currently writing can
only be done by one writer unless special care is taken (see **Performance Considerations** below).
* Google Cloud Storage is the only cloud store supported at this time
* Having multiple versions for an array can slow access time; this can be improved in the future
* (todo) Support cluster and inline solutions for automatically generating multi-scale representation of image data
* (todo) Simple helper function and executable script to save a list of files to the repo
* (todo) Potentially allow files and other meta to be set via a .git style directory system
* (todo) Create simple script to launch a single DicedStore to be shared by different processes.
* (todo) Simple script to ingest h5 file, stack of images into arrays

## Performance Considerations

* If one chooses a Google-backed DICED store, it should be possible to achieve high read throughput over several machines.  To do this, the user open multiple connections to DICED on different cluster nodes.
* Array data is stored as uniform smaller chunks internally.  When adding data to DICED, some writes can be inefficient as each write can result in reading data to reassemble the internal chunk.  For 3D array, data is partitioned into 64x64x64 blocks and optimal writing would ensure alignment to this partitioning.
* Parallel writes (even if spatially disjoint) can be dangerous because there is no protection if two processes write to the same internal chunk.  To circumvent this the user can ensure that writes are disjoint in the internal chunk space (such as the 64x64x64 blocks).
* Related to the previous, DICED does not allow multiple connections where metadata can change, such as the addition of new array objects.
