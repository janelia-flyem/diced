#!/bin/bash

# Make sure the dvid-console code is checked out.
git submodule init
git submodule update

# Build and install
$PYTHON setup.py install 



