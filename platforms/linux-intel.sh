#!/bin/bash

OPTS="$@"

export CC=icc
export CXX=icpc
export FC=ifort
export MPICC=mpiicc
export MPICXX=mpiicpc
export MPIFC=mpiifort
export CFLAGS="-O3 -g"
export CXXFLAGS="-O3 -g"
export FCFLAGS="-O3 -g"

./configure ${OPTS} \
--with-hdf5=/usr/bin/h5cc

